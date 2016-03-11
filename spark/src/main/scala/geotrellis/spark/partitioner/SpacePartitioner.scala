package geotrellis.spark.partitioner

import geotrellis.spark._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.zcurve.{Z3, Z2, ZGridKeyIndex}
import org.apache.spark._
import org.apache.spark.rdd.{ShuffledRDD, RDD}

import scala.collection.mutable.ArrayBuffer
import scala.reflect._

case class SpacePartitioner[K: Boundable](bounds: Bounds[K])
  (implicit index: PartitionerIndex[K]) extends Partitioner {

  val regions: Array[Long] =
    bounds match {
      case b: KeyBounds[K] =>
        for {
          (start, end) <- index.indexRanges(b).toArray
          p <- start to end
        } yield p

      case EmptyBounds =>
        Array.empty
    }

  def numPartitions = regions.length

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[GridKey]
    val x = index.asInstanceOf[PartitionerIndex[GridKey]]
    x.toIndex(k)

    val region = index.toIndex(key.asInstanceOf[K])
    val regionIndex = regions.indexOf(region)
    if (regionIndex > -1) regionIndex
    else (region % numPartitions).toInt // overflow for keys, at this point this should no longer be considered spatially partitioned
  }

  def containsKey(key: Any): Boolean = {
    val i = index.toIndex(key.asInstanceOf[K])
    regions.indexOf(i) > -1
  }

  def regionIndex(region: Long): Option[Int] = {
    // Note: Consider future design where region can overlap several partitions, would change Option -> List
    val i = regions.indexOf(region)
    if (i > -1) Some(i) else None
  }

  /**
    * Use this partitioner as a partitioner for rdd.
    * The rdd may have a SpacePartitioner already.
    * If it is in sync with Bounds in the Metadata we assume it to be valid .
    * Otherwise we assume it has degraded to be a hash partitioner and we must perform a shuffle.
    */
  def apply[V, M: Component[?, Bounds[K]]](rdd: RDD[(K, V)] with Metadata[M]) = {
    val kb: Bounds[K] = rdd.metadata.getComponent[Bounds[K]]
    rdd.partitioner match {
      case Some(part: SpacePartitioner[K]) if part.bounds == kb =>
        ContextRDD(
          new ReorderedSpaceRDD(rdd.filter(r => containsKey(r._1)), this),
          bounds)

      case _ =>
        ContextRDD(
          new ShuffledRDD(rdd.filter(r => containsKey(r._1)), this).asInstanceOf[RDD[(K, V)]],
          bounds)
    }
  }
}
