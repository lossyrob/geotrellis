package geotrellis.raster.mapalgebra.focal.hillshade

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster.mapalgebra.focal.Angles._

import scala.math._

/**
 * Computes Hillshade (shaded relief) from a raster.
 *
 * The resulting raster will be a shaded relief map (a hill shading)
 * based on the sun altitude, azimuth, and the z factor. The z factor is
 * a conversion factor from map units to elevation units.
 *
 * Returns a raster of ShortConstantNoDataCellType.
 *
 * This operation uses Horn's method for computing hill shading.
 *
 * If Hillshade operations encounters NoData in its neighborhood, that neighborhood cell well be treated as having
 * the same elevation as the focal cell.
 *
 * @see [[http://goo.gl/DtVDQ Esri Desktop's description of Hillshade.]]
 */
object Hillshade {
  def apply(tile: Tile, n: Neighborhood, bounds: Option[GridBounds], cs: CellSize, az: Double, al: Double, z: Double, target: TargetCell = TargetCell.All): Tile = {
    new SurfacePointCalculation[Tile](tile, n, bounds, cs, target) with
      ShortArrayTileResult
    {
      val azimuth = radians(90.0 - az)
      val zenith = radians(90.0 - al)
      val zFactor = z

      val cosZ = cos(zenith)
      val sinZ = sin(zenith)
      val cosAz = cos(azimuth)
      val sinAz = sin(azimuth)

      def setValue(x: Int, y: Int, s: SurfacePoint) {
        val slope = s.slope(zFactor)
        val aspect = s.aspect

        val c = cosAz * s.cosAspect + sinAz * s.sinAspect // cos(azimuth - aspect)
        val v = (cosZ * s.cosSlope) + (sinZ * s.sinSlope * c)
        resultTile.set(x, y, round(127.0 * max(0.0, v)).toInt)
      }
    }
  }.execute()

  /** Indirect calculation of hill shading of a tile that uses Aspect and Slope operation results */
  def indirect(aspect: Tile, slope: Tile, azimuth: Double, altitude: Double): Tile = {
    val az = radians(90.0 - azimuth)
    val ze = radians(90.0 - altitude)
    val cosZe = cos(ze)
    val sinZe = sin(ze)

    val hr = aspect.combineDouble(slope) { (aspectValue, slopeValue) =>
      val slopeRads = radians(slopeValue)
      val aspectRads = radians(aspectValue)
      val v = (cosZe * cos(slopeRads)) +
        (sinZe * sin(slopeRads) * cos(az - aspectRads))
      round(127.0 * max(0.0, v))
    }
    hr.convert(ShortConstantNoDataCellType)
  }
}
