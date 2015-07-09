package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.utils._
import geotrellis.raster._
import geotrellis.spark.io.hadoop._

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.accumulo.core.data.{Key, Mutation, Value, Range => ARange}
import org.apache.accumulo.core.client.mapreduce.{AccumuloOutputFormat, AccumuloFileOutputFormat}
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.conf.{AccumuloConfiguration, Property}

import scala.collection.JavaConversions._
import scala.collection.mutable

import scalaz.concurrent.Strategy
import scalaz.concurrent.Task
import scala.concurrent._
import scalaz.stream.async._
import scalaz.stream._
import spire.syntax.cfor._

sealed trait AccumuloWriteStrategy 

/**
 * This strategy will perfom Accumulo bulk ingest. Bulk ingest requires that sorted records be written to the 
 * filesystem, preferbly HDFS, before Accumulo is able to ingest them. After the ingest is finished
 * the nodes will likely go through a period of high load as they perform major compactions.
 *
 * Note: Giving relative URLs will cause HDFS to use the `fs.defaultFS` property in `core-site.xml`. 
 * If not specified this will default to local ('file:/') system, this is undesriable.
 *
 * @param ingestPath Path where spark will write RDD records for ingest
 */
case class HdfsWriteStrategy(ingestPath: Path) extends AccumuloWriteStrategy

/**
 * This strategy will create one BatchWriter per partition and attempt to stream the records to the target tablets.
 * In order to gain some parallism this strategy will create a number of splits in the target table equal to the number
 * of tservers in the cluster. This is suitable for smaller ingests, or where HdfsWriteStrategy is otherwise not possible.
 * 
 * There is a problem in Accumulo 1.6 (fixed in 1.7) where the split creation does not wait for the resulting
 * empty tablets to distribute through the cluster before returning. This will create a warm-up period where the
 * pressure the ingest writers on that node will delay tablet re-balancing.
 *
 * The speed of the ingest can be improved by setting `tserver.wal.sync.method=hflush` in accumulo shell.
 * Note: this introduces higher chance of data loss due to sudden node failure.
 *
 * BatchWriter is notified of the tablet migrations and will follow them around the cluster.
 *
 * @param config Configuration for the BatchWriters
 */
case class SocketWriteStrategy(
  config: BatchWriterConfig = new BatchWriterConfig().setMaxMemory(128*1024*1024).setMaxWriteThreads(32) 
) extends AccumuloWriteStrategy


trait RasterRDDWriter[K] {
  def rowId(id: LayerId, index: Long): String  
  
  def encode(
    layerId: LayerId,
    raster: RasterRDD[K],
    kIndex: KeyIndex[K]
  ): RDD[(Key, Value)]

  def write(
    instance: AccumuloInstance,
    layerMetaData: AccumuloLayerMetaData,
    keyBounds: KeyBounds[K],
    kIndex: KeyIndex[K]
  )(layerId: LayerId,
    raster: RasterRDD[K],
    strategy: AccumuloWriteStrategy
  )(implicit sc: SparkContext): Unit = {
    // Create table if it doesn't exist.
    val tileTable = layerMetaData.tileTable

    val ops = instance.connector.tableOperations()
    if (! ops.exists(tileTable)) 
      ops.create(tileTable)

    val groups = ops.getLocalityGroups(tileTable)
    val newGroup: java.util.Set[Text] = Set(new Text(layerId.name))
    ops.setLocalityGroups(tileTable, groups.updated(tileTable, newGroup))

    val kvPairs = encode(layerId, raster, kIndex)
    strategy match {
      case HdfsWriteStrategy(ingestPath) => {
        val job = Job.getInstance(sc.hadoopConfiguration)
        instance.setAccumuloConfig(job)
        val conf = job.getConfiguration
        val outPath = HdfsUtils.tmpPath(ingestPath, s"${layerId.name}-${layerId.zoom}", conf)
        val failuresPath = outPath.suffix("-failures")

        HdfsUtils.ensurePathExists(failuresPath, conf)
        kvPairs
          .sortBy{ case (key, _) => key }
          .saveAsNewAPIHadoopFile(
            outPath.toString,
            classOf[Key],
            classOf[Value],
            classOf[AccumuloFileOutputFormat],
            conf)

        ops.importDirectory(tileTable, outPath.toString, failuresPath.toString, true)          

        // cleanup ingest directories on success
        val fs = ingestPath.getFileSystem(conf)
        if( fs.exists(new Path(outPath, "_SUCCESS")) ) {
          fs.delete(outPath, true)
          fs.delete(failuresPath, true)
        } else {
          throw new LayerWriteError(layerId, s"Accumulo bulk ingest failed at $ingestPath")
        }
      }

      case SocketWriteStrategy(config: BatchWriterConfig) => {      
        // splits are required for efficient BatchWriter ingest
        val connector = instance.connector
        val tserverCount = math.max(connector.instanceOperations.getTabletServers.size, 1)
        val splitIndicies = getSplits(keyBounds, kIndex, tserverCount)
        ops.addSplits(
          tileTable, 
          new java.util.TreeSet(splitIndicies.map{ s => new Text(rowId(layerId, s)) })        
        )

        val bcVals = sc.broadcast((instance, config))
        kvPairs.foreachPartition { partition =>
          val (instance, config) = bcVals.value          
          val writer = instance.connector.createBatchWriter(tileTable, config)

          val mutations: Process[Task, Mutation] = 
            Process.unfold(partition){ iter => 
              if (iter.hasNext) {
                val (key, value) = iter.next
                val mutation = new Mutation(key.getRow)
                mutation.put(layerId.name, key.getColumnQualifier, System.currentTimeMillis(), value)
                Some(mutation, iter)
              } else  {
                None
              }
            }

          val writeChannel = channel.lift { 
            (mutation: Mutation) => Task { writer.addMutation(mutation) }
          }   
          
          val writes = mutations.tee(writeChannel)(tee.zipApply).map(Process.eval)
          nondeterminism.njoin(maxOpen = 32, maxQueued = 32)(writes).run.run
          writer.close
        }
      }
    }
  }

  /**
   * Mapping KeyBounds of Extent to SFC ranges will often result in a set of non-contigrious ranges.
   * The indices exluded by these ranges should not be included in split calcluation as they will never be seen.
   */
  def getSplits(kb: KeyBounds[K], ki: KeyIndex[K], count: Int): Seq[Long] = {  
    var stack = ki.indexRanges(kb).toList
    def len(r: (Long, Long)) = r._2 - r._1 + 1l
    val total = stack.foldLeft(0l){ (s,r) => s + len(r) }
    val binWidth = total / count
    
    def splitRange(range: (Long, Long), take: Long): ((Long, Long), (Long, Long)) = {
      assert(len(range) > take)
      assert(take > 0)
      (range._1, range._1 + take - 1) -> (range._1 + take, range._2)
    }

    val arr = Array.fill[Long](count - 1)(0)
    var sum = 0l
    var i = 0

    while (i < count - 1) {
      val nextStep = sum + len(stack.head)
      if (nextStep < binWidth){      
        sum += len(stack.head)
        stack = stack.tail
      } else if (nextStep == binWidth) {
        arr(i) = stack.head._2
        stack = stack.tail
        i += 1
        sum = 0l
      } else {
        val (take, left) = splitRange(stack.head, binWidth - sum)
        stack = left :: stack.tail
        arr(i) = take._2
        i += 1
        sum = 0l
      }
    }
    arr
  }
}
