package geotrellis

package object raster {
  final val byteNoData = Byte.MinValue
  final val shortNoData = Short.MinValue

  case class ByteNoDataCheck(i:Int) extends AnyVal {
    @inline final def isNoData = i == byteNoData
  }
  implicit def byteToNoDataCheck(b:Byte) = ByteNoDataCheck(i)

  case class ShortNoDataCheck(s:Short) extends AnyVal {
    @inline final def isNoData = s == shortNoData
  }
  implicit def shortToNoDataCheck(s:Short) = ShortNoDataCheck(s)

  case class FloatNoDataCheck(f:Float) extends AnyVal {
    @inline final def isNoData = f == java.lang.Float.isNaN(f)
  }
  implicit def floatToNoDataCheck(f:Short) = FloatNoDataCheck(f)


  @inline final def b2i(n:Byte):Int = if (n.isNoData) NODATA else n.toInt
  @inline final def i2b(n:Int):Byte = if (n.isNodata) byteNodata else n.toByte

  @inline final def b2d(n:Byte):Double = if (n.isNoData) Double.NaN else n.toDouble
  @inline final def d2b(n:Double):Byte = if (n.isNoData) byteNodata else n.toByte

  @inline final def s2i(n:Short):Int = if (n.isNoData) NODATA else n.toInt
  @inline final def i2s(n:Int):Short = if (n.isNoData) shortNodata else n.toShort

  @inline final def s2d(n:Short):Double = if (n.isNoData) Double.NaN else n.toDouble
  @inline final def d2s(n:Double):Short = if (n.isNoData) shortNodata else n.toShort

  @inline final def i2f(n:Int):Float = if (n.isNoData) Float.NaN else n.toFloat
  @inline final def f2i(n:Float):Int = if (n.isNoData) NODATA else n.toInt

  @inline final def i2d(n:Int):Double = if (n.isNoData) Double.NaN else n.toDouble
  @inline final def d2i(n:Double):Int = if (n.isNoData) NODATA else n.toInt  
}
