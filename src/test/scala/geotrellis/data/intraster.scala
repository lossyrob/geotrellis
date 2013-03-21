package geotrellis.data

import org.scalatest.FunSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.matchers.ShouldMatchers

import Console.printf
import geotrellis.process.TestServer
import geotrellis.{Extent,RasterExtent}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class RasterReaderSpec extends FunSpec with MustMatchers with ShouldMatchers {
  describe("An RasterReader") {
    it ("should work") {
      val e = Extent(-9.5, 3.8, 80 + -9.5, 80 + 3.8)
      val geo = RasterExtent(e, 8.0, 8.0, 10, 10)

      val server = TestServer()
      val raster = server.getRaster("src/test/resources/quad8.arg", None, Some(geo))

      val raster2 = RasterReader.read(raster, None)
      
      raster.equals(raster2) must be === true
      raster must be === raster2
    }
  }
}
