package geotrellis.spark

import geotrellis.spark._
import org.apache.spark.rdd.RDD
import spray.json._

/** A SpatialKey designates the spatial positioning of a layer's tile. */
case class AltSpatialKey(col: Int, row: Int, z: Double) extends Product3[Int, Int, Double] {
  def spatialKey: SpatialKey = SpatialKey(col, row)
  def _1 = col
  def _2 = row
  def _3 = z
}

object AltSpatialKey {
  implicit object SpatialComponent extends SpatialComponent[AltSpatialKey] {
    def lens = createLens(k => k.spatialKey, sk => k => AltSpatialKey(sk.col, sk.row, k.z))
  }

  implicit def ordering[A <: AltSpatialKey]: Ordering[A] =
    Ordering.by(sk => (sk.col, sk.row, sk.z))

  implicit val spatialKeyFormat = new RootJsonFormat[AltSpatialKey] {
    def write(key: AltSpatialKey) =
      JsObject(
        "col" -> JsNumber(key.col),
        "row" -> JsNumber(key.row),
        "z" -> JsNumber(key.z)
      )

    def read(value: JsValue): AltSpatialKey =
      value.asJsObject.getFields("col", "row", "z") match {
        case Seq(JsNumber(col), JsNumber(row), JsNumber(z)) =>
          AltSpatialKey(col.toInt, row.toInt, z.toDouble)
        case _ =>
          throw new DeserializationException("AltSpatialKey expected")
      }
  }

  implicit object Boundable extends Boundable[AltSpatialKey] {
    def minBound(a: AltSpatialKey, b: AltSpatialKey) = {
      AltSpatialKey(math.min(a.col, b.col), math.min(a.row, b.row), math.min(a.z, b.z))
    }
    def maxBound(a: AltSpatialKey, b: AltSpatialKey) = {
      AltSpatialKey(math.max(a.col, b.col), math.max(a.row, b.row), math.max(a.z, b.z))
    }

    def getKeyBounds(rdd: RDD[(AltSpatialKey, X)] forSome {type X}): KeyBounds[AltSpatialKey] = {
      rdd
        .map{ case (k, tile) => KeyBounds(k, k) }
        .reduce { combine }
    }
  }
}
