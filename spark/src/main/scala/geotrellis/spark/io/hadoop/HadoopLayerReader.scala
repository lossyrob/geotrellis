package geotrellis.spark.io.hadoop

import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.io.json._

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json._

import scala.reflect.ClassTag

/**
 * Handles reading raster RDDs and their metadata from S3.
 *
 * @param attributeStore  AttributeStore that contains metadata for corresponding LayerId
 * @tparam K              Type of RDD Key (ex: GridKey)
 * @tparam V       Type of RDD Value (ex: Tile or MultibandTile )
 * @tparam M              Type of Metadata associated with the RDD[(K,V)]
 */
class HadoopLayerReader(
  val attributeStore: AttributeStore[JsonFormat]
)(implicit sc: SparkContext)
  extends FilteringLayerReader[LayerId] with LazyLogging {

  val defaultNumPartitions = sc.defaultParallelism

  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](id: LayerId, rasterQuery: RDDQuery[K, M], numPartitions: Int, indexFilterOnly: Boolean): RDD[(K, V)] with Metadata[M] = {
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)
    val (header, metadata, keyIndex, writerSchema) = try {
      import spray.json.DefaultJsonProtocol._
      attributeStore.readLayerAttributes[HadoopLayerHeader, M, KeyIndex[K], Schema](id)
    } catch {
      case e: AttributeNotFoundError => throw new LayerReadError(id).initCause(e)
    }

    val layerPath = header.path
    val keyBounds = metadata.getComponent[Bounds[K]].getOrElse(throw new LayerEmptyBoundsError(id))
    val queryKeyBounds = rasterQuery(metadata)

    val rdd: RDD[(K, V)] =
      if (queryKeyBounds == Seq(keyBounds)) {
        HadoopRDDReader.readFully(layerPath, Some(writerSchema))
      } else {
        val decompose = (bounds: KeyBounds[K]) => keyIndex.indexRanges(bounds)
        HadoopRDDReader.readFiltered(layerPath, queryKeyBounds, decompose, indexFilterOnly, Some(writerSchema))
      }

    new ContextRDD[K, V, M](rdd, metadata)
  }
}

object HadoopLayerReader {
  def apply(attributeStore: HadoopAttributeStore)(implicit sc: SparkContext) =
    new HadoopLayerReader(attributeStore)

  def apply(rootPath: Path)(implicit sc: SparkContext): HadoopLayerReader =
    apply(HadoopAttributeStore(rootPath))
}
