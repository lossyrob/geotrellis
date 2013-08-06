package geotrellis.raster.op.local

import geotrellis._
import geotrellis.raster.RasterUtil._

/**
 * Generate a raster with the values from the first raster, but only include
 * cells in which the corresponding cell in the second raster *are not* set to the 
 * "readMask" value. 
 *
 * For example, if *all* cells in the second raster are set to the readMask value,
 * the output raster will be empty -- all values set to NODATA.
 */
case class Mask(r1:Op[Raster], r2:Op[Raster], readMask:Op[Int], writeMask:Op[Int]) 
  extends Op4(r1,r2,readMask,writeMask) ({
  (r1,r2,readMask,writeMask) => AndThen(logic.RasterDualCombine(r1,r2)
    ((z1,z2) => if (z2 == readMask) writeMask else z1)
    ((z1,z2) => if (d2i(z2) == readMask) i2d(writeMask) else z1))
})
