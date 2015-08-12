package geotrellis.raster.io.geotiff.reader

import geotrellis.raster.io.geotiff._
import geotrellis.proj4._

import org.scalatest._

class GeoTiffCRSReaderSpec extends FunSpec
    with Matchers {

  describe("reading a CRS out of a GeoTiff") {

    it("should read a WebMercator CRS if the geotiff is in WebMercator") {
      val crs = SingleBandGeoTiff.compressed("raster-test/data/reproject/slope_webmercator.tif").crs
      println(s"READ CRS: ${crs.toProj4String}")
      println(s"TARGET CRS: ${WebMercator.toProj4String}")
      crs should be (WebMercator)
    }
  }
}
