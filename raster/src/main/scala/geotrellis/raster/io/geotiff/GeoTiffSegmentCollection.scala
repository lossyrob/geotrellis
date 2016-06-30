package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression._

trait GeoTiffSegmentCollection {
  type T >: Null <: GeoTiffSegment

  val compressedBytes: SegmentBytes
  val decompressor: Decompressor

  val bandType: BandType

  def getDecompressedBytes(i: Int): Array[Byte] =
    decompressor.decompress(compressedBytes.getSegment(i), i)

  // Cached last segment
  private var _lastSegment: T = null
  private var _lastSegmentIndex: Int = -1

  val createSegment: Int => T

  def getSegment(i: Int): T = {
    if(i != _lastSegmentIndex) {
      _lastSegment = createSegment(i)
      _lastSegmentIndex = i
    }
    _lastSegment
  }

}
