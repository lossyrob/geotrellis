package geotrellis.spark

import com.github.nscala_time.time.Imports._
import geotrellis.proj4._
import geotrellis.vector._

/** A key for a Tile with time as well as spatial dimension */
case class TimeProjectedExtent(extent: Extent, crs: CRS, instant: Long) {
  def time: DateTime = new DateTime(instant, DateTimeZone.UTC)
  def projectedExtent = ProjectedExtent(extent, crs)
}

object TimeProjectedExtent {
  def apply(extent: Extent, crs: CRS, time: DateTime): TimeProjectedExtent =
    TimeProjectedExtent(extent, crs, time.getMillis)

  def apply(projectedExtent: ProjectedExtent, time: DateTime): TimeProjectedExtent =
    TimeProjectedExtent(projectedExtent.extent, projectedExtent.crs, time.getMillis)

  def apply(projectedExtent: ProjectedExtent, instant: Long): TimeProjectedExtent =
    TimeProjectedExtent(projectedExtent.extent, projectedExtent.crs, instant)

  implicit val projectedExtentComponent =
    Component[TimeProjectedExtent, ProjectedExtent](
      k => k.projectedExtent,
      (k, pe) => TimeProjectedExtent(pe, k.instant)
    )

  implicit val timeComponent =
    Component[TimeProjectedExtent, TimeKey](
      k => TimeKey(k.instant),
      (k, tk) => TimeProjectedExtent(k.extent, k.crs, tk.instant)
    )
}
