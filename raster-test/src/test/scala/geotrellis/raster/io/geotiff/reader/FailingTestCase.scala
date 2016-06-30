package geotrellis.raster.io.geotiff.reader

import geotrellis.raster._
import geotrellis.raster.io.arg._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.util._
import geotrellis.raster.io.geotiff.tags._
import geotrellis.raster.summary.polygonal._

import geotrellis.vector.{Point, Extent}
import geotrellis.raster.testkit._
import geotrellis.proj4._

import monocle.syntax.apply._
import org.scalactic.Tolerance

import scala.io.{Source, Codec}
import scala.collection.immutable.HashMap

import java.util.BitSet
import java.nio.ByteBuffer

import spire.syntax.cfor._
import org.scalatest._

class FailingTestCase extends FunSuite
    with RasterMatchers
    with GeoTiffTestUtils {

  test("Fails to read correct extent") {
    val actual = SinglebandGeoTiff.compressed(new java.io.File(baseDataPath, "read-geotiff-bug.tif").getAbsolutePath).extent
    // The expected Extent is read in from gdalinfo
    val expected = Extent( 0.5000000,   0.5000000,
                          10.5000000,  10.5000000)

    actual should be (expected)
  }
}
