package geotrellis.spark.mapalgebra.focal

import geotrellis.spark._

import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster._

import org.scalatest.FunSpec

class MedianSpec extends FunSpec with TestEnvironment {

  describe("Median Focal Spec") {

    val nd = NODATA

    it("should square median for raster rdd") {
      val rasterRDD = createTileLayerRDD(
        sc,
        ArrayTile(Array(
          nd,7, 1,   1, 3, 5,   9, 8, 2,
          9, 1, 1,   2, 2, 2,   4, 3, 5,

          3, 8, 1,   3, 3, 3,   1, 2, 2,
          2, 4, 7,   1,nd, 1,   8, 4, 3
        ), 9, 4),
        TileLayout(3, 2, 3, 2)
      )

      val res = rasterRDD.focalMedian(Square(1)).stitch.toArray

      val expected = Array(
        7, 1, 1,    1, 2, 3,    4, 4, 4,
        7, 2, 1,    2, 3, 3,    3, 3, 2,

        3, 3, 2,    2, 2, 2,    3, 3, 3,
        3, 3, 3,    3, 3, 3,    2, 2, 2
      )

      res should be (expected)
    }

  }

}
