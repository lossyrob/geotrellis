package geotrellis.raster.io.geotiff

import geotrellis.util._
import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster.testkit._

import org.scalatest._
import scala.collection.mutable._
import spire.syntax.cfor._

class BufferSegmentBytesSpec extends FunSpec 
  with Matchers
  with BeforeAndAfterAll
  with RasterMatchers
  with GeoTiffTestUtils
  with TileBuilders {

    describe("Abstracting a ByteBuffer with BufferSegmentBytes") {
      it("should return the correct offset and byteCount for the given intersecting segment") {
        val extent = Extent(0,0,100,100)
        val geotiff = SinglebandGeoTiff(s"$baseDataPath/econic.tif")
        
        val buffer = Filesystem.toMappedByteBuffer(s"$baseDataPath/econic.tif")
        val tiffTags = TiffTagsReader.read(s"$baseDataPath/econic.tif")

        val segmentBytes = new BufferSegmentBytes(buffer, extent, tiffTags)

        val offsets = {
          if (tiffTags.hasStripStorage)
            tiffTags.basicTags.stripOffsets.get
          else
            tiffTags.tileTags.tileOffsets.get
        }

        val byteCounts = {
          if (tiffTags.hasStripStorage)
            tiffTags.basicTags.stripByteCounts.get
          else
            tiffTags.tileTags.tileByteCounts.get
        }

        val segmentArray = segmentBytes.intersectingSegments.toArray
        val segment = segmentArray(0)

        val (expectedOffset, expectedByteCount) = (segmentBytes.intersectingOffsets(0), segmentBytes.intersectingByteCounts(0))
        val (actualOffset, actualByteCount) = (offsets(segment), byteCounts(segment))

        (expectedOffset, expectedByteCount) shouldEqual (actualOffset, actualByteCount)
      }

      it("should have a tail that contains the next segment") {
        val extent = Extent(0,0,2500,2500)
        val buffer = Filesystem.toMappedByteBuffer(s"$baseDataPath/econic.tif")
        val tiffTags = TiffTagsReader.read(s"$baseDataPath/econic.tif")

        val segmentBytes = new BufferSegmentBytes(buffer, extent, tiffTags)
        val reader = new segmentBytes.BufferSegmentBytesReader(0)
        val tail = reader.tail
        val expected = tail.head
        
        expected should not equal reader.head
      }

     it("should contain all of the intersecting segments") {
       val extent = Extent(0,0,100,100)
       val buffer = Filesystem.toMappedByteBuffer(s"$baseDataPath/econic.tif")
       val tiffTags = TiffTagsReader.read(s"$baseDataPath/econic.tif")

       val segmentBytes = new BufferSegmentBytes(buffer, extent, tiffTags)
       val expected = segmentBytes

       val windowed = SinglebandGeoTiff.windowed(s"$baseDataPath/econic.tif", extent)
       val actual = windowed.imageData.compressedBytes

       actual shouldEqual expected
     }

     it("striped, SinglebandGeoTiff") {
       val extent = Extent(0,-5,20,-4)

       val whole = SinglebandGeoTiff(geoTiffPath("uncompressed/striped/byte.tif"))
       val wholeBytes = whole.imageData.compressedBytes.size

       val windowed = SinglebandGeoTiff.windowed(geoTiffPath("uncompressed/striped/byte.tif"), extent)
       val windowedBytes = windowed.imageData.compressedBytes.size

       wholeBytes should not equal windowedBytes
     }

     it("tiled, SinglebnadGeoTiff") {
       val extent = Extent(0,-5,20,-4)

        val geotiff = GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-striped-pixel.tif"))
        val segmentLayout = geotiff.imageData.segmentLayout
        val tileLayout = segmentLayout.tileLayout
        val rasterExtent = RasterExtent(geotiff.extent, tileLayout.layoutCols, tileLayout.layoutRows)
        val gridBounds = rasterExtent.gridBoundsFor(extent)
        val coords = gridBounds.coords
        val segments = Set[Int]()

        cfor(0)(_ < coords.size, _ + 1) { i =>
          segments += segmentLayout.getSegmentIndex(coords(i)._1, coords(i)._2)
        }
       val whole = SinglebandGeoTiff(geoTiffPath("uncompressed/tiled/byte.tif"))
       val wholeBytes = whole.imageData.compressedBytes.size

       val windowed = SinglebandGeoTiff.windowed(geoTiffPath("uncompressed/tiled/byte.tif"), extent)
       val windowedBytes = windowed.imageData.compressedBytes.size

       wholeBytes should not equal windowedBytes
     }

      it("striped, Multiband") {
        val extent = Extent(0,0,100,100)

        val geotiff = GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-striped-pixel.tif"))
        val segmentLayout = geotiff.imageData.segmentLayout
        val tileLayout = segmentLayout.tileLayout
        val rasterExtent = RasterExtent(geotiff.extent, tileLayout.layoutCols, tileLayout.layoutRows)
        val gridBounds = rasterExtent.gridBoundsFor(extent)
        val coords = gridBounds.coords
        val segments = Set[Int]()

        cfor(0)(_ < coords.size, _ + 1) { i =>
          segments += segmentLayout.getSegmentIndex(coords(i)._1, coords(i)._2)
        }

        val windowed = MultibandGeoTiff.windowed((geoTiffPath("3bands/int32/3bands-striped-pixel.tif")), extent)
        val segmentBytes = windowed.imageData.compressedBytes.size

        segmentBytes shouldEqual segments.size
      }
      
      it("tiled, Multiband") {
        val extent = Extent(0,0,100,100)

        val geotiff = GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-tiled-pixel.tif"))
        val segmentLayout = geotiff.imageData.segmentLayout
        val tileLayout = segmentLayout.tileLayout
        val rasterExtent = RasterExtent(geotiff.extent, tileLayout.layoutCols, tileLayout.layoutRows)
        val gridBounds = rasterExtent.gridBoundsFor(extent)
        val coords = gridBounds.coords
        val segments = Set[Int]()

        cfor(0)(_ < coords.size, _ + 1) { i =>
          segments += segmentLayout.getSegmentIndex(coords(i)._1, coords(i)._2)
        }

        val windowed = MultibandGeoTiff.windowed((geoTiffPath("3bands/int32/3bands-tiled-pixel.tif")), extent)
        val segmentBytes = windowed.imageData.compressedBytes.size

        segmentBytes shouldEqual segments.size
      }
    }
  }
