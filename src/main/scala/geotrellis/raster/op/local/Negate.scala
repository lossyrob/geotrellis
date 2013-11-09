package geotrellis.raster.op.local

import geotrellis._
import geotrellis.process._

/**
 * Negate (multiply by -1) each value in a raster.
 */
object Negate extends Serializable {
  def apply(r:Op[Raster]) = 
    r.map(_.dualMap(z => if(z.isNoData) z else -z)
                   (z => if(z.isNoData) z else -z))
     .withName("Negate[Raster]")
}
