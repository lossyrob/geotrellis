package geotrellis.raster.op.local

import geotrellis._
import geotrellis.source._

/**
 * Pows values.
 * 
 * @note        NoData values will cause the results of this operation
 *              to be NODATA or Double.NaN.
 */
object Pow extends LocalRasterBinaryOp {
  def combine(z1:Int,z2:Int) =
    if (z1.isNoData || z2.isNoData) NODATA
    else if (z2 == 0) NODATA
    else math.pow(z1,z2).toInt

  def combine(z1:Double,z2:Double) =
    math.pow(z1,z2)
}

trait PowOpMethods[+Repr <: RasterSource] { self: Repr =>
  /** Pow each value of the raster by a constant value.*/
  def localPow(i: Int) = self.mapOp(Pow(_, i))
  /** Pow each value of the raster by a constant value.*/
  def **(i:Int) = localPow(i)
  /** Pow a constant value by each cell value.*/
  def localPowValue(i:Int) = self.mapOp(Pow(i,_))
  /** Pow a constant value by each cell value.*/
  def **:(i:Int) = localPow(i)
  /** Pow each value of a raster by a double constant value.*/
  def localPow(d: Double) = self.mapOp(Pow(_, d))
  /** Pow each value of a raster by a double constant value.*/
  def **(d:Double) = localPow(d)
  /** Pow a double constant value by each cell value.*/
  def localPowValue(d:Double) = self.mapOp(Pow(d,_))
  /** Pow a double constant value by each cell value.*/
  def **:(d:Double) = localPowValue(d)
  /** Pow the values of each cell in each raster. */
  def localPow(rs:RasterSource) = self.combineOp(rs)(Pow(_,_))
  /** Pow the values of each cell in each raster. */
  def **(rs:RasterSource) = localPow(rs)
  /** Pow the values of each cell in each raster. */
  def localPow(rss:Seq[RasterSource]) = self.combineOp(rss)(Pow(_))
  /** Pow the values of each cell in each raster. */
  def **(rss:Seq[RasterSource]) = localPow(rss)
}
