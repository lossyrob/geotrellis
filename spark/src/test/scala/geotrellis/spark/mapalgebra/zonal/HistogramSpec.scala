package geotrellis.spark.mapalgebra.zonal

import Implicits._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.testfiles._
import geotrellis.raster._
import geotrellis.raster.histogram.Histogram

import geotrellis.vector._

import org.scalatest.FunSpec

import collection._

class HistogramSpec extends FunSpec with TestEnvironment with TestFiles {

  describe("Histogram Zonal Operation") {
    it("gives correct histogram for example raster rdds") {
      val rdd = createTileLayerRDD(
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

      val zonesRDD = createTileLayerRDD(
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

      val r = rdd.stitch
      val zones = zonesRDD.stitch
      val (cols, rows) = (r.cols, r.rows)

      val zoneValues = mutable.Map[Int, mutable.ListBuffer[Int]]()

      for(row <- 0 until rows) {
        for(col <- 0 until cols) {
          val z = zones.get(col, row)
          if(!zoneValues.contains(z)) zoneValues(z) = mutable.ListBuffer[Int]()
          zoneValues(z) += r.get(col, row)
        }
      }

      val expected =
        zoneValues.toMap.mapValues { list =>
          list.distinct
            .map { v => (v, list.filter(_ == v).length) }
            .toMap
        }

      val result: Map[Int, Histogram[Int]] = rdd.zonalHistogram(zonesRDD)
      result.keys should be (expected.keys)

      for(zone <- result.keys) {
        val hist = result(zone)
        for(v <- expected(zone).keys) {
          hist.getItemCount(v) should be (expected(zone)(v))
        }
      }
    }

  }

}
