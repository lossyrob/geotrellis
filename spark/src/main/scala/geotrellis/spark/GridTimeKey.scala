package geotrellis.spark

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import spray.json._
import com.github.nscala_time.time.Imports._

case class GridTimeKey(col: Int, row: Int, instant: Long) {
  def spatialKey: GridKey = GridKey(col, row)
  def timeKey: TimeKey = TimeKey(time)
  def time: DateTime = new DateTime(instant, DateTimeZone.UTC)
}

object GridTimeKey {
  def apply(spatialKey: GridKey, timeKey: TimeKey): GridTimeKey =
    GridTimeKey(spatialKey.col, spatialKey.row, timeKey.time)

  def apply(col: Int, row: Int, dateTime: DateTime): GridTimeKey =
    GridTimeKey(col, row, dateTime.getMillis)

  implicit val spatialComponent =
    Component[GridTimeKey, GridKey](k => k.spatialKey, (k, sk) => GridTimeKey(sk.col, sk.row, k.time))

  implicit val timeComponent =
    Component[GridTimeKey, TimeKey](k => k.timeKey, (k, tk) => GridTimeKey(k.col, k.row, tk.instant))

  implicit def ordering: Ordering[GridTimeKey] =
    Ordering.by(stk => (stk.spatialKey, stk.timeKey))

  implicit object Boundable extends Boundable[GridTimeKey] {
    def minBound(a: GridTimeKey, b: GridTimeKey) = {
      GridTimeKey(math.min(a.col, b.col), math.min(a.row, b.row), if (a.time < b.time) a.time else b.time )
    }

    def maxBound(a: GridTimeKey, b: GridTimeKey) = {
      GridTimeKey(math.max(a.col, b.col), math.max(a.row, b.row), if (a.time > b.time) a.time else b.time )
    }
  }
}
