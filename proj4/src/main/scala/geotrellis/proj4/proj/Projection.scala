package geotrellis.proj4.proj

import geotrellis.proj4.ProjCoordinate
import geotrellis.proj4.util._

trait Projection {
  /** Projects a geographic point (in degrees), producing a projected result, (in the units of the target coordinate system). */
  def project(x: Double, y: Double): (Double, Double)
  /** Inverse-projects a point (in the units defined by the coordinate system) producing a geographic result (in degrees) */
  def inverseProject(x: Double, y: Double)

  /** Transform a geographic point (in radians),  producing a projected result (in the units of the target coordinate system). */
  def projectRadians(x: Double, y: Double): (Double, Double)
  /** Inverse-transforms a point (in the units defined by the coordinate system), producing a geographic result (in radians) */
  def inverseProjectRadians(lplam: Double, lpphi: Double): ProjCoordinate

  def name: String
  def epsg: Int
}


 // {
 //    val x1 = x*DTR;
 //    if ( projectionLongitude != 0 )
 //            x = ProjectionMath.normalizeLongitude( x-projectionLongitude );
 //        return projectRadians(x, src.y*DTR, dst);


// trait ZeroLongitude {
//   def projectDegrees(x: Double, y: Double): (Double, Double) = (x * DTR, y * DTR)
//   def projectDegrees(x: Double, y: Double): (Double, Double) = (x * RTD, y * RTD)
// }

// trait Longitude {
//   def projectionLongitude: Double

//   def projectDegrees(x: Double, y: Double): (Double, Double) = (x * DTR, y * DTR)
//   def projectDegrees(x: Double, y: Double): (Double, Double) = (x * RTD, y * RTD)
// }
