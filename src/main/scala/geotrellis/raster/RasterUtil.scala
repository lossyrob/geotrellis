package geotrellis.raster

import geotrellis._

/**
 * These can now be imported through the package object geotrellis.raster, e.g.
 * 
 * import geotrellis.raster._
 * 
 * The RasterUtil object contains a bunch of final values and methods used for
 * no data checks and conversions. It's important to avoid using toInt and
 * toDouble when converting raster values, since these methods don't have
 * NODATA/Double.NaN conversion correctly.
 * 
 */
object RasterUtil {
  @inline final def b2i(n:Byte):Int = n.toRasterInt
  @inline final def i2b(n:Int):Byte = n.toRasterByte

  @inline final def s2i(n:Short):Int = n.toRasterInt
  @inline final def i2s(n:Int):Short = n.toRasterShort

  @inline final def i2f(n:Int):Float = n.toRasterFloat
  @inline final def f2i(n:Float):Int = n.toRasterInt

  @inline final def i2d(n:Int):Double = n.toRasterDouble
  @inline final def d2i(n:Double):Int = n.toRasterInt
}
