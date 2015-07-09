package geotrellis.raster.reproject

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.engine._
import geotrellis.testkit._
import geotrellis.proj4._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader._

import org.scalatest._
import spire.syntax.cfor._

class ReprojectSpec extends FunSpec
    with TileBuilders
    with TestEngine {
  describe("reprojects in approximation to GDAL") {

    it("should (approximately) match a GDAL nearest neighbor interpolation on nlcd tile") {
      val ProjectedRaster(source, extent, crs) = SingleBandGeoTiff("raster-test/data/reproject/nlcd_tile_wsg84.tif").projectedRaster

      val Raster(expected, expectedExtent) = 
        SingleBandGeoTiff("raster-test/data/reproject/nlcd_tile_webmercator-nearestneighbor.tif").raster
 
      val Raster(actual, actualExtent) =
        source.reproject(extent, crs, WebMercator, ReprojectOptions(NearestNeighbor, 0.0))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }

    it("should (approximately) match a GDAL nearest neighbor interpolation on slope tif") {
      val Raster(source, extent) = 
        SingleBandGeoTiff("raster-test/data/reproject/slope_webmercator.tif").raster

      val Raster(expected, expectedExtent) = 
        SingleBandGeoTiff("raster-test/data/reproject/slope_wsg84-nearestneighbor.tif").raster

      val Raster(actual, actualExtent) =
        source.reproject(extent, WebMercator, LatLng, ReprojectOptions(NearestNeighbor, 0.0))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }

    it("should (approximately) match a GDAL nearest neighbor interpolation on slope tif and an error threshold of 0.125") {
      val Raster(source, extent) = 
        SingleBandGeoTiff("raster-test/data/reproject/slope_webmercator.tif").raster

      val Raster(expected, expectedExtent) = 
        SingleBandGeoTiff("raster-test/data/reproject/slope_wsg84-nearestneighbor-er0.125.tif").raster

      val Raster(actual, actualExtent) =
        source.reproject(extent, WebMercator, LatLng, ReprojectOptions(NearestNeighbor, 0.125))

      actual.rows should be (expected.rows)
      actual.cols should be (expected.cols)

      actualExtent.xmin should be (expectedExtent.xmin +- 0.00001)
      actualExtent.xmax should be (expectedExtent.xmax +- 0.00001)
      actualExtent.ymax should be (expectedExtent.ymax +- 0.00001)
      actualExtent.ymin should be (expectedExtent.ymin +- 0.00001)

      cfor(0)(_ < actual.rows - 1, _ + 1) { row =>
        cfor(0)(_ < actual.cols - 1, _ + 1) { col =>
          withClue(s"Failed on ($col, $row): ") {
            actual.getDouble(col, row) should be (expected.getDouble(col, row))
          }
        }
      }
    }
  }
}
