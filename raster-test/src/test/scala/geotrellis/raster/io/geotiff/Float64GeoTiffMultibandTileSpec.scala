package geotrellis.raster.io.geotiff

import geotrellis.raster._
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.mapalgebra.local._

import geotrellis.vector.Extent

import geotrellis.proj4._

import geotrellis.raster.testkit._

import org.scalatest._

class Float64GeoTiffMultibandTileSpec extends FunSpec
    with Matchers
    with BeforeAndAfterAll
    with RasterMatchers
    with GeoTiffTestUtils 
    with TileBuilders {
  def p(s: String, i: String): String = 
    geoTiffPath(s"3bands/float64/3bands-${s}-${i}.tif")

  describe("Float64GeoTiffMultibandTile") {

    // Combine all bands, double

    it("should combine all bands with pixel interleave, striped") {
      val tile =
        MultibandGeoTiff.compressed(p("striped", "pixel")).tile

      val actual = tile.combineDouble(_.sum)
      val expected = RawArrayTile(Array.ofDim[Double](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with pixel interleave, tiled") {
      val tile =
        MultibandGeoTiff.compressed(p("tiled", "pixel")).tile

      val actual = tile.combineDouble(_.sum)
      val expected = RawArrayTile(Array.ofDim[Double](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with band interleave, striped") {
      val tile =
        MultibandGeoTiff.compressed(p("striped", "band")).tile

      val actual = tile.combineDouble(_.sum)
      val expected = RawArrayTile(Array.ofDim[Double](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }

    it("should combine all bands with band interleave, tiled") {
      val tile =
        MultibandGeoTiff.compressed(p("tiled", "band")).tile

      val actual = tile.combineDouble(_.sum)
      val expected = RawArrayTile(Array.ofDim[Double](tile.cols * tile.rows).fill(6), tile.cols, tile.rows)

      assertEqual(actual, expected)
    }
  }
}
