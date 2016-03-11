package geotrellis.spark

import geotrellis.spark.io.json.Implicits._

import com.github.nscala_time.time.Imports._
import spray.json._

object TimeKey {
  def apply(dateTime: DateTime): TimeKey =
    TimeKey(dateTime.getMillis)

  implicit def dateTimeToKey(time: DateTime): TimeKey =
    TimeKey(time)

  implicit def keyToDateTime(key: TimeKey): DateTime =
    key.time

  implicit def ordering[A <: TimeKey]: Ordering[A] =
    Ordering.by(tk => tk.instant)

}

/** A TimeKey designates the time positioning of a layer's tile. */
case class TimeKey(instant: Long) {
  def time: DateTime = new DateTime(instant, DateTimeZone.UTC)
}
