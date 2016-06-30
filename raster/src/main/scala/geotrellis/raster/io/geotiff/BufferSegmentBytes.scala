package geotrellis.raster.io.geotiff

import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.tags._
import geotrellis.raster.io.geotiff.util._

import scala.collection.mutable._
import java.nio.ByteBuffer
import monocle.syntax.apply._
import spire.syntax.cfor._

class BufferSegmentBytes(byteBuffer: ByteBuffer, storageMethod: StorageMethod, tiffTags: TiffTags) extends SegmentBytes {
  val (offsets, byteCounts) =
    storageMethod match {
      case _: Striped =>

        val stripOffsets = (tiffTags &|->
          TiffTags._basicTags ^|->
          BasicTags._stripOffsets get)

        val stripByteCounts = (tiffTags &|->
          TiffTags._basicTags ^|->
          BasicTags._stripByteCounts get)

        (stripOffsets.get, stripByteCounts.get)

      case _: Tiled =>
        val tileOffsets = (tiffTags &|->
          TiffTags._tileTags ^|->
          TileTags._tileOffsets get)

        val tileByteCounts = (tiffTags &|->
          TiffTags._tileTags ^|->
          TileTags._tileByteCounts get)

        (tileOffsets.get, tileByteCounts.get)
    }

  override val size: Int = offsets.size

  def getSegment(i: Int): Array[Byte] = {
    val oldOffset = byteBuffer.position
    byteBuffer.position(offsets(i))
    val result = byteBuffer.getSignedByteArray(byteCounts(i))
    byteBuffer.position(oldOffset)
    result
  }
}
