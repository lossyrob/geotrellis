package geotrellis.spark.mapalgebra.focal.hillshade

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.hillshade._

import geotrellis.spark._

import geotrellis.vector.Extent

import org.scalatest._

import spire.syntax.cfor._

class HillshadeSpec extends FunSpec with TestEnvironment {

  describe("Hillshade Elevation Spec") {

    it("should get the same result on elevation for spark op as single raster op") {
      val rasterOp = (tile: Tile, re: RasterExtent) => tile.hillshade(re.cellSize)
      val sparkOp = (rdd: TileLayerRDD[GridKey]) => rdd.hillshade()

      val path = "aspect.tif"

      testGeoTiff(sc, path)(rasterOp, sparkOp)
    }
  }
}
