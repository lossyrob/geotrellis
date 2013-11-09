package geotrellis

package object raster {

  @inline final def b2i(n:Byte):Int = n.toRasterInt
  @inline final def i2b(n:Int):Byte = n.toRasterByte

  @inline final def b2d(n:Byte):Double = n.toRasterDouble
  @inline final def d2b(n:Double):Byte = n.toRasterByte

  @inline final def s2i(n:Short):Int = n.toRasterInt
  @inline final def i2s(n:Int):Short = n.toRasterShort

  @inline final def s2d(n:Short):Double = n.toRasterDouble
  @inline final def d2s(n:Double):Short = n.toRasterShort

  @inline final def i2f(n:Int):Float = n.toRasterFloat
  @inline final def f2i(n:Float):Int = n.toRasterInt

  @inline final def i2d(n:Int):Double = n.toRasterDouble
  @inline final def d2i(n:Double):Int = n.toRasterInt
}
