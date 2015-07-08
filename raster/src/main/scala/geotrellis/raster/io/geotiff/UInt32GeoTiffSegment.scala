package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.utils._

import java.nio.ByteBuffer
import java.util.BitSet

import spire.syntax.cfor._

class NoDataUInt32GeoTiffSegment(bytes: Array[Byte], noDataValue: Float) extends UInt32GeoTiffSegment(bytes) {
  override
  def get(i: Int): Float = {
    val v = super.get(i)
    if(v == noDataValue) { Float.NaN }
    else { v }
  }
}

class UInt32GeoTiffSegment(val bytes: Array[Byte]) extends GeoTiffSegment {
  protected val buffer = ByteBuffer.wrap(bytes).asIntBuffer

  val size: Int = bytes.size / 4

  def get(i: Int): Float = 
    (buffer.get(i) & 0xFFFFFFFFL).toFloat

  def getInt(i: Int): Int = f2i(get(i))
  def getDouble(i: Int): Double = f2d(get(i))

  def convert(cellType: CellType): Array[Byte] =
    cellType match {
      case TypeBit =>
        val bs = new BitSet(size)
        cfor(0)(_ < size, _ + 1) { i => if ((getInt(i) & 1) == 0) { bs.set(i) } }
        bs.toByteArray()
      case TypeByte | TypeUByte => 
        val arr = Array.ofDim[Byte](size)
        cfor(0)(_ < size, _ + 1) { i => arr(i) = f2b(get(i)) }
        arr
      case TypeShort | TypeUShort =>
        val arr = Array.ofDim[Short](size)
        cfor(0)(_ < size, _ + 1) { i => arr(i) = f2s(get(i)) }
        arr.toArrayByte()
      case TypeInt =>
        val arr = Array.ofDim[Int](size)
        cfor(0)(_ < size, _ + 1) { i => arr(i) = getInt(i) }
        arr.toArrayByte()
      case TypeFloat =>
        val arr = Array.ofDim[Float](size)
        cfor(0)(_ < size, _ + 1) { i => arr(i) = get(i) }
        arr.toArrayByte()
      case TypeDouble =>
        val arr = Array.ofDim[Double](size)
        cfor(0)(_ < size, _ + 1) { i => arr(i) = getDouble(i) }
        arr.toArrayByte()
    }

  // NOTE: Maps to Float32 bytes.
  def map(f: Int => Int): Array[Byte] = {
    val arr = Array.ofDim[Float](size)
    cfor(0)(_ < size, _ + 1) { i =>
      arr(i) = i2f(f(getInt(i)))
    }
    val result = new Array[Byte](size * TypeFloat.bytes)
    val bytebuff = ByteBuffer.wrap(result)
    bytebuff.asFloatBuffer.put(arr)
    result
  }

  def mapDouble(f: Double => Double): Array[Byte] = {
    val arr = Array.ofDim[Float](size)
    cfor(0)(_ < size, _ + 1) { i =>
      arr(i) = d2f(f(getDouble(i)))
    }
    val result = new Array[Byte](size * TypeFloat.bytes)
    val bytebuff = ByteBuffer.wrap(result)
    bytebuff.asFloatBuffer.put(arr)
    result
  }

  def mapWithIndex(f: (Int, Int) => Int): Array[Byte] = {
    val arr = Array.ofDim[Float](size)
    cfor(0)(_ < size, _ + 1) { i =>
      arr(i) = i2f(f(i, getInt(i)))
    }
    val result = new Array[Byte](size * TypeFloat.bytes)
    val bytebuff = ByteBuffer.wrap(result)
    bytebuff.asFloatBuffer.put(arr)
    result
  }

  def mapDoubleWithIndex(f: (Int, Double) => Double): Array[Byte] = {
    val arr = Array.ofDim[Float](size)
    cfor(0)(_ < size, _ + 1) { i =>
      arr(i) = d2f(f(i, getDouble(i)))
    }
    val result = new Array[Byte](size * TypeFloat.bytes)
    val bytebuff = ByteBuffer.wrap(result)
    bytebuff.asFloatBuffer.put(arr)
    result
  }
}
