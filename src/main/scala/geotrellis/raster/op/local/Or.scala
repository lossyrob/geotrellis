package geotrellis.raster.op.local

import geotrellis._
import geotrellis.raster._
import geotrellis.source._

/**
 * Or's cell values of rasters or Int values.
 *
 * @note        NoData values will cause the results of this operation
 *              to be NODATA.
 * @note        If used with Double typed rasters, the values
 *              will be rounded to Ints.
 */
object Or extends LocalRasterBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (z1.isNoData || z2.isNoData) NODATA
    else z1 | z2

  def combine(z1:Double,z2:Double) =
    if (z1.isNoData || z2.isNoData) Double.NaN
    else i2d(d2i(z1) | d2i(z2))
}

trait OrOpMethods[+Repr <: RasterSource] { self: Repr =>
  /** Or a constant Int value to each cell. */
  def localOr(i: Int) = self.mapOp(Or(_, i))
  /** Or a constant Int value to each cell. */
  def |(i:Int) = localOr(i)
  /** Or a constant Int value to each cell. */
  def |:(i:Int) = localOr(i)
  /** Or the values of each cell in each raster.  */
  def localOr(rs:RasterSource) = self.combineOp(rs)(Or(_,_))
  /** Or the values of each cell in each raster. */
  def |(rs:RasterSource) = localOr(rs)
  /** Or the values of each cell in each raster.  */
  def localOr(rss:Seq[RasterSource]) = self.combineOp(rss)(Or(_))
  /** Or the values of each cell in each raster. */
  def |(rss:Seq[RasterSource]) = localOr(rss)
}
