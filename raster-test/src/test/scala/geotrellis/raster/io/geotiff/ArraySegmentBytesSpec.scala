package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.reader._
import geotrellis.raster.testkit._

import org.scalatest._

class ArraySegmentBytesSpec extends FunSpec 
  with Matchers
  with BeforeAndAfterAll
  with RasterMatchers
  with GeoTiffTestUtils
  with TileBuilders {

    describe("Abstracting an Array[Array[Byte]] with ArraySegmentBytes") {
      it("should contain the same number of segments that are in the Singleband file") {
        val geotiff = SinglebandGeoTiff(s"$baseDataPath/econic.tif")
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)
        val expected = arraySegmentBytes.size
        val actual = geotiff.imageData.compressedBytes.size

        assert(expected == actual)
      }

      it("should contain the same number of segments that are in the Multiband file") {
        val geotiff = GeoTiffReader.readMultiband(geoTiffPath("3bands/int32/3bands-striped-pixel.tif"))
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)
        val expected = arraySegmentBytes.size
        val actual = geotiff.imageData.compressedBytes.size

        assert(expected == actual)
      }

      it("should produce the same Byte when it is called using a Singleband") {
        val geotiff = SinglebandGeoTiff.compressed(geoTiffPath("1band/aspect_bit_uncompressed_striped.tif"))
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)
        val expected = arraySegmentBytes.getSegment(0)
        val actual = geotiff.imageData.compressedBytes(0)

        assert(expected == actual)
      }

      it("should produce the same Byte when it is called using a Multiband") {
        val geotiff = SinglebandGeoTiff.compressed(geoTiffPath("3bands/3bands-tiled-deflate.tif"))
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)
        val expectedArray = arraySegmentBytes.getSegment(0)
        val actualArray = geotiff.imageData.compressedBytes(0)

        val expected = expectedArray(0)
        val actual = actualArray(0)

        assert(expected == actual)
      }

      it("should produce the same segment when it is crated from a ByteBuffer using a Singleband") {
        val geotiff = SinglebandGeoTiff.compressed(geoTiffPath("3bands/3bands-tiled-deflate.tif"))
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)
        val expectedArray = arraySegmentBytes.getSegment(0)
        val actualArray = geotiff.imageData.compressedBytes(0)

        val expected = expectedArray(0)
        val actual = actualArray(0)

        assert(expected == actual)
      }

      it("should be of type SegmentBytes") {
        val geotiff = SinglebandGeoTiff(s"$baseDataPath/econic.tif")
        val arraySegmentBytes = new ArraySegmentBytes(geotiff.imageData.compressedBytes)

        val actual =
          arraySegmentBytes match {
            case (s: SegmentBytes) => true
            case _ => false
          }

        assert(actual)
      }
    }
  }
