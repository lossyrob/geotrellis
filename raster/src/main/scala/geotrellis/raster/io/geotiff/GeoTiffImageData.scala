package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression._

trait GeoTiffImageData {
  def cols: Int
  def rows: Int
  def bandType: BandType
  def bandCount: Int
  def compressedBytes: SegmentBytes
  def decompressor: Decompressor
  def segmentLayout: GeoTiffSegmentLayout
}
