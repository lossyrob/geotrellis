package geotrellis.spark.io

import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.json._

import org.apache.spark.rdd._
import spray.json._

import scala.reflect._

abstract class FilteringLayerReader[ID] extends LayerReader[ID] {

  /** read
    *
    * This function will read an RDD layer based on a query.
    *
    * @param id              The ID of the layer to be read
    * @param rasterQuery     The query that will specify the filter for this read.
    * @param numPartitions   The desired number of partitions in the resulting RDD.
    * @param indexFilterOnly If true, the reader should only filter out elements who's KeyIndex entries
    *                        do not match the indexes of the query key bounds. This can include keys that
    *                        are not inside the query key bounds.
    * @tparam K              Type of RDD Key (ex: GridKey)
    * @tparam V              Type of RDD Value (ex: Tile or MultibandTile )
    * @tparam M              Type of Metadata associated with the RDD[(K,V)]

    */
  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](id: ID, rasterQuery: RDDQuery[K, M], numPartitions: Int, indexFilterOnly: Boolean): RDD[(K, V)] with Metadata[M]

  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](id: ID, rasterQuery: RDDQuery[K, M], numPartitions: Int): RDD[(K, V)] with Metadata[M] =
    read(id, rasterQuery, numPartitions, false)

  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](id: ID, rasterQuery: RDDQuery[K, M]): RDD[(K, V)] with Metadata[M] =
    read(id, rasterQuery, defaultNumPartitions)

  def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](id: ID, numPartitions: Int): RDD[(K, V)] with Metadata[M] =
    read(id, new RDDQuery[K, M], numPartitions)

  def query[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](layerId: ID): BoundRDDQuery[K, M, RDD[(K, V)] with Metadata[M]] =
    new BoundRDDQuery(new RDDQuery, read(layerId, _))

  def query[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: Component[?, Bounds[K]]
  ](layerId: ID, numPartitions: Int): BoundRDDQuery[K, M, RDD[(K, V)] with Metadata[M]] =
    new BoundRDDQuery(new RDDQuery, read(layerId, _, numPartitions))
}
