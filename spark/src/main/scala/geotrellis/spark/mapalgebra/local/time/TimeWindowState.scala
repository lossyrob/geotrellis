package geotrellis.spark.mapalgebra.local.time

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import reflect.ClassTag


object TimeWindowHelper {

  val UnitSeconds = 1
  val UnitMinutes = 2
  val UnitHours = 3
  val UnitDays = 4
  val UnitWeeks = 5
  val UnitMonths = 6
  val UnitYears = 7

  val Average = 1
  val Minimum = 2
  val Maximum = 3
  val Variance = 4

  def badState = throw new IllegalStateException("Bad time window method state.")

  def parseUnit(s: String) = s.toLowerCase match {
    case "seconds" => UnitSeconds
    case "minutes" => UnitMinutes
    case "hours" => UnitHours
    case "days" => UnitDays
    case "weeks" => UnitWeeks
    case "months" => UnitMonths
    case "years" => UnitYears
    case _ => throw new IllegalArgumentException("Unknown unit: $s.")
  }

}

case class TimeWindowState[K](
  rdd: RDD[(K, Tile)],
  method: Int,
  windowSize: Option[Int] = None,
  unit: Option[Int] = None,
  start: Option[DateTime] = None,
  partitioner: Option[Partitioner] = None
)(
  implicit val keyClassTag: ClassTag[K],
    _sc: GridComponent[K],
    _tc: TimeComponent[K]) {

  import TimeWindowHelper._

  private lazy val state =
    if (windowSize.isEmpty && unit.isEmpty) 0
    else if (start.isEmpty) 1
    else 2

  def per(p: Int)(unitString: String): TimeWindowState[K] =
    if (state != 0) badState
    else {
      val u = parseUnit(unitString)
      copy(windowSize = Some(p), unit = Some(u))
    }

  def from(s: DateTime): TimeWindowState[K] =
    if (state != 1) badState
    else copy(start = Some(s))

  def to(to: DateTime) =
    if (state != 2) badState
    else method match {
      case Average => rdd.timeMean(windowSize.get, unit.get, start.get, to, partitioner)
      case Minimum => rdd.timeMin(windowSize.get, unit.get, start.get, to, partitioner)
      case Maximum => rdd.timeMax(windowSize.get, unit.get, start.get, to, partitioner)
      case Variance => rdd.timeVariance(windowSize.get, unit.get, start.get, to, partitioner)
      case _ => throw new IllegalStateException("Bad method $method.")
    }

}
