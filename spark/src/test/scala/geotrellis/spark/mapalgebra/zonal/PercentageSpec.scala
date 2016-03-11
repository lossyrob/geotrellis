package geotrellis.spark.mapalgebra.zonal

import Implicits._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testfiles._

import geotrellis.raster._
import geotrellis.raster.stitch._
import geotrellis.raster.mapalgebra.zonal._

import geotrellis.vector._

import org.scalatest.FunSpec
import org.apache.spark.rdd.RDD
import spire.syntax.cfor._

class PercentageSpec extends FunSpec with TestEnvironment with TestFiles {

  describe("Percentage Zonal Operation") {
    it("gives correct percentage for example raster rdds") {
      val rdd: RDD[(GridKey, Tile)] = createTileLayerRDD(
        sc,
        ArrayTile(Array(
          1, 2, 2,  2, 3, 1,  6, 5, 1,
          1, 2, 2,  2, 3, 6,  6, 5, 5,

          1, 3, 5,  3, 6, 6,  6, 5, 5,
          3, 1, 5,  6, 6, 6,  6, 6, 2,

          7, 7, 5,  6, 1, 3,  3, 3, 2,
          7, 7, 5,  5, 5, 4,  3, 4, 2,

          7, 7, 5,  5, 5, 4,  3, 4, 2,
          7, 2, 2,  5, 4, 4,  3, 4, 4), 9, 8),
        TileLayout(3, 4, 3, 2)
      )

      val zonesRDD: RDD[(GridKey, Tile)] = createTileLayerRDD(
        sc,
        ArrayTile(Array(
          1, 1, 1,  4, 4, 4,  5, 6, 6,
          1, 1, 1,  4, 4, 5,  5, 6, 6,

          1, 1, 2,  4, 5, 5,  5, 6, 6,
          1, 2, 2,  3, 3, 3,  3, 3, 3,

          2, 2, 2,  3, 3, 3,  3, 3, 3,
          2, 2, 2,  7, 7, 7,  7, 8, 8,

          2, 2, 2,  7, 7, 7,  7, 8, 8,
          2, 2, 2,  7, 7, 7,  7, 8, 8), 9, 8),
        TileLayout(3, 4, 3, 2)
      )

      val actual = rdd.zonalPercentage(zonesRDD).stitch
      val expected = rdd.stitch.zonalPercentage(zonesRDD.stitch)

      (actual.cols, actual.rows) should be (expected.cols, expected.rows)

      val (cols, rows) = (actual.cols, actual.rows)

      cfor(0)(_ < rows, _ + 1) { row =>
        cfor(0)(_ < cols, _ + 1) { col =>
          val actualValue = actual.getDouble(col, row)
          val expectedValue = expected.getDouble(col, row)
          actualValue should be (expectedValue)
        }
      }
    }

  }

}
