package geotrellis.data.geotiff

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream

import scala.collection.mutable
import scala.annotation.switch

import scala.math.{ceil, min}

import geotrellis._
import geotrellis.raster.RasterUtil._

object RawEncoder {
  def render(encoder:Encoder) = RawEncoder(encoder).render()

  def apply(encoder:Encoder) = encoder.settings match {
    case Settings(IntSample, Floating, _, _) => new RawFloatEncoder(encoder)
    case Settings(LongSample, Floating, _, _) => new RawDoubleEncoder(encoder)
    case Settings(ByteSample, _, _, _) => new RawByteEncoder(encoder)
    case Settings(ShortSample, _, _, _) => new RawShortEncoder(encoder)
    case Settings(IntSample, _, _, _) => new RawIntEncoder(encoder)
    case s => sys.error("can't encoder %s" format s)
  }
}

class RawByteEncoder(encoder:Encoder) extends RawEncoder(encoder) {
  def handleCell(i:Int) {
    var z = data.apply(i)
    if (z.isNoData) z = encoder.noDataInt
    dmg.writeByte(z)
  }
}

class RawShortEncoder(encoder:Encoder) extends RawEncoder(encoder) {
  def handleCell(i:Int) {
    var z = data.apply(i)
    if (z.isNoData) z = encoder.noDataInt
    dmg.writeShort(z)
  }
}

class RawIntEncoder(encoder:Encoder) extends RawEncoder(encoder) {
  def handleCell(i:Int) {
    var z = data.apply(i)
    if (z.isNoData) z = encoder.noDataInt
    dmg.writeInt(z)
  }
}

class RawFloatEncoder(encoder:Encoder) extends RawEncoder(encoder) {
  val ndf = if (encoder.settings.esriCompat) Float.MinValue else Float.NaN
  def handleCell(i:Int) {
    var z = data(i).toRasterDouble
    dmg.writeFloat(if (z.isNoData) ndf else z.toFloat)
  }
}

class RawDoubleEncoder(encoder:Encoder) extends RawEncoder(encoder) {
  val ndf = if (encoder.settings.esriCompat) Double.MinValue else Double.NaN
  def handleCell(i:Int) {
    var z = data(i).toRasterDouble
    dmg.writeDouble(if (z.isNoData) ndf else z)
  }
}

abstract class RawEncoder(encoder:Encoder) {
  val cols = encoder.cols
  val rows = encoder.rows
  val data = encoder.data
  val dmg = new DataOutputStream(encoder.img)

  def handleCell(i:Int): Unit

  def render():(Array[Int], Array[Int]) = {
    var row = 0

    while (row < rows) {
      val rowspan = row * cols
      var col = 0
      while (col < cols) {
        handleCell(rowspan + col)
        col += 1
      }
      row += 1
    }

    val offsets = Array.ofDim[Int](encoder.numStrips)
    val lengths = Array.ofDim[Int](encoder.numStrips)
    val last = encoder.numStrips - 1
    for (i <- 0 until last) {
      offsets(i) = encoder.imageStartOffset + i * encoder.bytesPerStrip
      lengths(i) = encoder.bytesPerStrip
    }
    offsets(last) = encoder.imageStartOffset + last * encoder.bytesPerStrip
    lengths(last) = encoder.bytesPerRow * encoder.leftOverRows

    (offsets, lengths)
  }
}
