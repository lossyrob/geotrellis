package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression._

import java.nio.ByteBuffer

class UInt32GeoTiffMultibandTile(
  compressedBytes: SegmentBytes,
  decompressor: Decompressor,
  segmentLayout: GeoTiffSegmentLayout,
  compression: Compression,
  bandCount: Int,
  hasPixelInterleave: Boolean,
  val cellType: FloatCells with NoDataHandling
) extends GeoTiffMultibandTile(compressedBytes, decompressor, segmentLayout, compression, bandCount, hasPixelInterleave)
    with UInt32GeoTiffSegmentCollection {

  protected def createSegmentCombiner(targetSize: Int): SegmentCombiner =
    new SegmentCombiner(bandCount) {
      private val arr = Array.ofDim[Float](targetSize)

      def set(targetIndex: Int, v: Int): Unit = {
        arr(targetIndex) = i2f(v)
      }

      def setDouble(targetIndex: Int, v: Double): Unit = {
        arr(targetIndex) = d2f(v)
      }

      def getBytes(): Array[Byte] = {
        val result = new Array[Byte](targetSize * FloatConstantNoDataCellType.bytes)
        val bytebuff = ByteBuffer.wrap(result)
        bytebuff.asFloatBuffer.put(arr)
        result
      }
    }

  def withNoData(noDataValue: Option[Double]): UInt32GeoTiffMultibandTile =
    new UInt32GeoTiffMultibandTile(compressedBytes, decompressor, segmentLayout, compression, bandCount, hasPixelInterleave, cellType.withNoData(noDataValue))


  def interpretAs(newCellType: CellType): GeoTiffMultibandTile  = {
    newCellType match {
      case dt: FloatCells with NoDataHandling =>
        new UInt32GeoTiffMultibandTile(compressedBytes, decompressor, segmentLayout, compression, bandCount, hasPixelInterleave, dt)
      case _ =>
        withNoData(None).convert(newCellType)
    }
  }
}

