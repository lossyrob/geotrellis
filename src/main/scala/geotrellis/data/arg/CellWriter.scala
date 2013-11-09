package geotrellis.data.arg

import java.io.DataOutputStream

import geotrellis._
import geotrellis.data._
import geotrellis.util._
import geotrellis.process._

object CellWriter {
  def byType(typ:RasterType): CellWriter = typ match {
    case TypeBit => BoolCellWriter
    case TypeByte => Int8CellWriter
    case TypeShort => Int16CellWriter
    case TypeInt => Int32CellWriter
    case TypeFloat => Float32CellWriter
    case TypeDouble => Float64CellWriter
    case t => sys.error("raster type %s is not supported yet" format t)
  }
}

trait CellWriter {
  protected[this] def writeCell(raster:Raster, col:Int, row:Int, cols:Int, dos:DataOutputStream)

  def writeCells(raster:Raster, dos:DataOutputStream) {
    val cols = raster.rasterExtent.cols
    val rows = raster.rasterExtent.rows

    var row = 0
    while (row < rows) {
      var col = 0
      while (col < cols) {
        writeCell(raster, col, row, cols, dos)
        col += 1
      }
      row += 1
    }
  }
}

object BoolCellWriter extends CellWriter {
  override def writeCells(raster:Raster, dos:DataOutputStream) {
    val cols = raster.rasterExtent.cols
    val rows = raster.rasterExtent.rows

    var i = 0
    var z = 0
    var row = 0
    while (row < rows) {
      var col = 0
      while (col < cols) {
        z += (raster.get(col, row) & 1) << i
        i += 1
        col += 1
        if (i > 7) {
          dos.writeByte(z)
          i = 0
          z = 0
        }
      }
      row += 1
    }
    if(i != 0) { dos.writeByte(z) } // Write last byte!
  }
  def writeCell(raster:Raster, col:Int, row:Int, cols:Int, dos:DataOutputStream) = ()
}

trait IntCellWriter extends CellWriter {
  def noDataValue:Int
  def writeValue(z:Int, dos:DataOutputStream): Unit
  @inline final def writeCell(raster:Raster, col:Int, row:Int, cols:Int, dos:DataOutputStream) {
    val z = raster.get(col, row)
    if (z.isNoData) writeValue(noDataValue, dos) else writeValue(z, dos)
  }
}

trait FloatCellWriter extends CellWriter {
  def writeValue(z:Double, dos:DataOutputStream): Unit
  @inline final def writeCell(raster:Raster, col:Int, row:Int, cols:Int, dos:DataOutputStream) {
    writeValue(raster.getDouble(col, row), dos)
  }
}

object Int8CellWriter extends IntCellWriter {
  @inline final def noDataValue = NODATA.byte
  @inline final def writeValue(z:Int, dos:DataOutputStream) { dos.writeByte(z) }
}

object Int16CellWriter extends IntCellWriter {
  @inline final def noDataValue = NODATA.short
  @inline final def writeValue(z:Int, dos:DataOutputStream) { dos.writeShort(z) }
}

object Int32CellWriter extends IntCellWriter {
  @inline final def noDataValue = NODATA.int
  @inline final def writeValue(z:Int, dos:DataOutputStream) { dos.writeInt(z) }
}

object Float32CellWriter extends FloatCellWriter {
  @inline final def writeValue(z:Double, dos:DataOutputStream) { dos.writeFloat(z.toFloat) }
}

object Float64CellWriter extends FloatCellWriter {
  @inline final def writeValue(z:Double, dos:DataOutputStream) { dos.writeDouble(z) }
}
