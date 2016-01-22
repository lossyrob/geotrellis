package geotrellis.spark.io.file

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._

import org.scalatest._

abstract class FileSpatialSpec
    extends PersistenceSpec[SpatialKey, Tile, RasterMetaData]
    with TestEnvironment
    with TestFiles
    with AllOnesTestTileTests {
  lazy val reader = FileLayerReader[SpatialKey, Tile, RasterMetaData](outputLocalPath)
  lazy val deleter = FileLayerDeleter[SpatialKey, Tile, RasterMetaData](outputLocalPath)
  lazy val copier = FileLayerCopier[SpatialKey, Tile, RasterMetaData](outputLocalPath)
  lazy val mover  = FileLayerMover[SpatialKey, Tile, RasterMetaData](outputLocalPath)
  lazy val reindexer = FileLayerReindexer[SpatialKey, Tile, RasterMetaData](outputLocalPath, ZCurveKeyIndexMethod)
  lazy val tiles = FileTileReader[SpatialKey, Tile](outputLocalPath)
  lazy val sample = AllOnesTestFile
}

class FileSpatialRowMajorSpec extends FileSpatialSpec {
  lazy val writer = FileLayerWriter[SpatialKey, Tile, RasterMetaData](outputLocalPath, RowMajorKeyIndexMethod)
}

class FileSpatialZCurveSpec extends FileSpatialSpec {
  lazy val writer = FileLayerWriter[SpatialKey, Tile, RasterMetaData](outputLocalPath, ZCurveKeyIndexMethod)
}

class FileSpatialHilbertSpec extends FileSpatialSpec {
  lazy val writer = FileLayerWriter[SpatialKey, Tile, RasterMetaData](outputLocalPath, HilbertKeyIndexMethod)
}


import org.apache.spark.rdd._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.hilbert._
import org.apache.avro._
import org.apache.avro.generic._


class AltSpatialKeyIndex(spatialKeyIndex: KeyIndex[SpatialKey]) extends KeyIndex[AltSpatialKey] {
  def toIndex(key: AltSpatialKey): Long = spatialKeyIndex.toIndex(key.spatialComponent)
  def indexRanges(keyRange: (AltSpatialKey, AltSpatialKey)): Seq[(Long, Long)] =
    spatialKeyIndex.indexRanges((keyRange._1.spatialComponent, keyRange._2.spatialComponent))
}



class AltFileSpatialSpec
    extends FunSpec with Matchers with TestEnvironment
    with TestFiles {

  implicit def spatialKeyIndexIndex(m: HilbertKeyIndexMethod): KeyIndexMethod[AltSpatialKey] =
    new KeyIndexMethod[AltSpatialKey] {
      def createIndex(keyBounds: KeyBounds[AltSpatialKey]): KeyIndex[AltSpatialKey] = {
        val xResolution = resolution(keyBounds.maxKey.row - keyBounds.minKey.row)
        val yResolution = resolution(keyBounds.maxKey.col - keyBounds.minKey.col)
        val zResolution = 3
        new HilbertAltSpatialKeyIndex(keyBounds, xResolution, yResolution, zResolution)

        // val skb = KeyBounds(keyBounds._1.spatialComponent, keyBounds._2.spatialComponent)
        // new AltSpatialKeyIndex(HilbertSpatialKeyIndex(skb, xResolution, yResolution))
      }
    }


  implicit def altSpatialKeyAvroFormat = new AvroRecordCodec[AltSpatialKey] {
    def schema = SchemaBuilder
      .record("SpatialKey").namespace("geotrellis.spark")
      .fields()
      .name("col").`type`().intType().noDefault()
      .name("row").`type`().intType().noDefault()
      .name("z").`type`().doubleType().noDefault()
      .endRecord()

    def encode(key: AltSpatialKey, rec: GenericRecord) = {
      rec.put("row", key.row)
      rec.put("col", key.col)
      rec.put("z", key.z)
    }

    def decode(rec: GenericRecord): AltSpatialKey =
      AltSpatialKey(
        rec[Int]("col"),
        rec[Int]("row"),
        rec[Double]("z")
      )

  }



  lazy val sample: RDD[(SpatialKey, Tile)] with Metadata[RasterMetaData] = AllOnesTestFile

  lazy val writer = FileLayerWriter[AltSpatialKey, Tile, RasterMetaData](outputLocalPath, HilbertKeyIndexMethod)

  lazy val reader = FileLayerReader[AltSpatialKey, Tile, RasterMetaData](outputLocalPath)

  object AltitudeBetween {
    def apply[T](start: T, end: T) = RDDFilter.Value[AltitudeBetween.type, (T, T)](start -> end)

    /** Define Between filter for a tuple of DateTimes */
    implicit def forAltitudeTuple[M] =
      new RDDFilter[AltSpatialKey, AltitudeBetween.type, (Double, Double), M] {
        def apply(metadata: M, kb: KeyBounds[AltSpatialKey], range: (Double, Double)) = {
          val queryBounds =
            KeyBounds(
              AltSpatialKey(kb.minKey.col, kb.minKey.row, range._1),
              AltSpatialKey(kb.maxKey.col, kb.maxKey.row, range._2)
            )
          println(kb)
          println(queryBounds)
          println(implicitly[Boundable[AltSpatialKey]].intersect(queryBounds, kb))
          implicitly[Boundable[AltSpatialKey]].intersect(queryBounds, kb).toSeq
        }
      }
  }


  describe("asdf") {
    it("asdfasd") {
      val altKeyed: RDD[(AltSpatialKey, Tile)] with Metadata[RasterMetaData] =
        sample
          .withContext { rdd =>
            rdd.flatMap { case (key, value) =>
              Seq(
                (AltSpatialKey(key.col, key.row, 10.0), value),
                (AltSpatialKey(key.col, key.row, 20.0), value)
              )
            }
          }

      val id = LayerId("whocares", 0)
      writer.write(id, altKeyed)

      val rdd: RDD[(AltSpatialKey, Tile)] with Metadata[RasterMetaData] =
        reader.query(id)
          .where(AltitudeBetween(5.0, 15.0))
          .toRDD

      println(sample.count)
      println(rdd.map(_._1).collect.toSeq)
    }
  }
}
