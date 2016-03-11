package geotrellis.spark.mapalgebra.local.time

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.rdd.RDD
import reflect.ClassTag

object Implicits extends Implicits

trait Implicits  {

  implicit class withLocalTimeTileRDDMethods[K](val self: RDD[(K, Tile)])(
    implicit val keyClassTag: ClassTag[K],
    implicit val _sc: GridComponent[K],
    implicit val _tc: TimeComponent[K]) extends LocalTimeTileRDDMethods[K] { }

  implicit class TimeWindow[K](val self: RDD[(K, Tile)])(
    implicit val keyClassTag: ClassTag[K],
    _sc: GridComponent[K],
    _tc: TimeComponent[K]) {

    import TimeWindowHelper._

    def average: TimeWindowState[K] = TimeWindowState(self, Average)

    def minimum: TimeWindowState[K] = TimeWindowState(self, Minimum)

    def maximum: TimeWindowState[K] = TimeWindowState(self, Maximum)

    def variance: TimeWindowState[K] = TimeWindowState(self, Variance)
  }
}
