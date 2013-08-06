package geotrellis.data.arg

import geotrellis._
import geotrellis.data._
import geotrellis.data.arg._
import geotrellis._
import geotrellis.process._
import geotrellis.testutil._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import geotrellis.raster.BitArrayRasterData

@RunWith(classOf[JUnitRunner])
class Int1Test extends FunSuite {
  val server = TestServer.server

  val arr = Array[Byte]((1 + 0 + 4 + 0 + 0 + 32 + 64 + 128).toByte,
                        (0 + 0 + 0 + 8 + 16 + 0 + 64 + 128).toByte)

  val data = BitArrayRasterData(arr, 4, 4)
  val e = Extent(10.0, 11.0, 14.0, 15.0)
  val re = RasterExtent(e, 1.0, 1.0, 4, 4)
  val raster = Raster(data, re)

  def loadRaster(path:String) = server.run(io.LoadFile(path))

  test("write out the bit raster") {
    ArgWriter(TypeBit).write("/tmp/foo-bool.arg", raster, "foo-bool")
  }

  test("check int8") {
    val r = loadRaster("/tmp/foo-bool.arg")
    assert(r.data === raster.data)
  }
}
