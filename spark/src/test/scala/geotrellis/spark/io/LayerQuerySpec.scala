/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark.tiling._
import geotrellis.spark.testfiles._
import geotrellis.spark.testkit._

import org.scalatest._

class TestItOut extends FunSpec with Matchers {
  import geotrellis.raster.io.geotiff._
  import geotrellis.vector.io._
  import geotrellis.spark._
  import geotrellis.spark.io._
  import geotrellis.spark.io.slippy._

  import java.io.File

  val e = s"""{"type": "Polygon", "coordinates": [[[1450606.9863003485, 6874761.1777096335], [1453551.0983858046, 6874841.041727237], [1453630.1872897947, 6871890.04526937], [1450687.1503113946, 6871810.256835365], [1450606.9863003485, 6874761.1777096335]]]}""".parseGeoJson[Polygon].envelope

  describe("ISPRS hacks") {
    it("georeference the fcn dsm result tifs") {
      val toref = "/Users/rob/proj/az/deepml/tmpdata/fcndsm/"
      val fromref = "/Users/rob/proj/az/deepml/tmpdata/fcn/"
      val result = "/Users/rob/proj/az/deepml/tmpdata/fcn-res/"

      val Coords = """.*(\d?\d)_(\d?\d).*""".r

      val toRefMap =
        (new File(toref).listFiles().map { f =>
          val Coords(x, y) = f.getName
          println(f)
          ((x, y), f.getAbsolutePath)
        }).toMap

      val fromRefMap =
        (new File(fromref).listFiles().map { f =>
          val Coords(x, y) = f.getName
          ((x, y), f.getAbsolutePath)
        }).toMap

      for((key, path) <- toRefMap) {
        val original = MultibandGeoTiff.compressed(path)
        val refpath = fromRefMap(key)
        println(refpath)
        val (ext, crs) = {
          val ref = MultibandGeoTiff.streaming(refpath)
          (ref.extent, ref.crs)
        }
        val n = new File(path).getName.replace(".tif", "-geo.tif")
        val rpath = s"${result}/$n"
        println(s"Writing $rpath")
        GeoTiff(original.tile, ext, crs).write(rpath)
      }
    }

    ignore("normalized dsm") {
      val b = "/Users/rob/proj/az/deepml/data/geotrellis-dsm-tiles/"
      val b2 = "/Users/rob/proj/az/deepml/data/"
      val b3 = "/Users/rob/data/datasets/isprs/potsdam/DSM_normalisation_with_geotrellis_byte/"

      val l = Seq(
        "dsm_potsdam_02_10.tff",
        "dsm_potsdam_02_11.tff",
        "dsm_potsdam_02_12.tff",
        "dsm_potsdam_03_10.tff",
        "dsm_potsdam_03_11.tff",
        "dsm_potsdam_03_12.tff",
        "dsm_potsdam_04_10.tff",
        "dsm_potsdam_04_11.tff",
        "dsm_potsdam_04_12.tff",
        "dsm_potsdam_05_10.tff",
        "dsm_potsdam_05_11.tff",
        "dsm_potsdam_05_12.tff",
        "dsm_potsdam_06_07.tff",
        "dsm_potsdam_06_08.tff",
        "dsm_potsdam_06_09.tff",
        "dsm_potsdam_06_10.tff",
        "dsm_potsdam_06_11.tff",
        "dsm_potsdam_06_12.tff",
        "dsm_potsdam_07_07.tff",
        "dsm_potsdam_07_08.tff",
        "dsm_potsdam_07_09.tff",
        "dsm_potsdam_07_10.tff",
        "dsm_potsdam_07_11.tff",
        "dsm_potsdam_07_12.tff").map { x => s"${b}${x}" }

      // val mapzens = Seq(
      //   "mapzen_tile_0-rp.tif",
      //   "mapzen_tile_1-rp.tif",
      //   "mapzen_tile_10-rp.tif",
      //   "mapzen_tile_11-rp.tif",
      //   "mapzen_tile_12-rp.tif",
      //   "mapzen_tile_13-rp.tif",
      //   "mapzen_tile_14-rp.tif",
      //   "mapzen_tile_15-rp.tif",
      //   "mapzen_tile_2-rp.tif",
      //   "mapzen_tile_3-rp.tif",
      //   "mapzen_tile_4-rp.tif",
      //   "mapzen_tile_5-rp.tif",
      //   "mapzen_tile_6-rp.tif",
      //   "mapzen_tile_7-rp.tif",
      //   "mapzen_tile_8-rp.tif",
      //   "mapzen_tile_9-rp.tif"
      // ).map { x => s"${b2}${x}" }
      // .map(SinglebandGeoTiff(_)).toArray
      val mapzen = SinglebandGeoTiff(s"${b2}mapzen.tif")

      import spire.syntax.cfor._
      import geotrellis.raster.resample._

      val res = new BilinearResample(mapzen.raster.tile, mapzen.raster.extent)

      val minMaxes  = scala.collection.mutable.ListBuffer[(Double, Double)]()

      for(path <- l) {
        val gt = SinglebandGeoTiff(path)
        val tile = gt.raster.tile
        val adjusted =
          tile.mapDouble { (col, row, z) =>
            val (x, y) = gt.raster.rasterExtent.gridToMap(col, row)
            val (mc, mr) = mapzen.raster.rasterExtent.mapToGrid(x, y)

            val mz =
              if(mc < 0 || mr < 0 || mapzen.raster.tile.cols <= mc || mapzen.raster.tile.rows <= mr) {
                println("No Match!")
                println(s"${(x, y)}")
                0.0
              } else {
                res.resampleDouble(x, y)
              }

            z - mz
          }

        val (min, max) = adjusted.findMinMaxDouble
        println(s"$path = min max ${(min, max)}")
        minMaxes += min -> max
      }

      val min = minMaxes.map(_._1).min
      val max = minMaxes.map(_._2).max

      for(path <- l) {
        val gt = SinglebandGeoTiff(path)
        val tile = gt.raster.tile
        val adjusted =
          tile.mapDouble { (col, row, z) =>
            val (x, y) = gt.raster.rasterExtent.gridToMap(col, row)
            val (mc, mr) = mapzen.raster.rasterExtent.mapToGrid(x, y)

            val mz =
              if(mc < 0 || mr < 0 || mapzen.raster.tile.cols <= mc || mapzen.raster.tile.rows <= mr) {
                println("No Match!")
                println(s"${(x, y)}")
                0.0
              } else {
                res.resampleDouble(x, y)
              }

            z - mz
          }

        val rescaled =
          adjusted.delayedConversion(UByteCellType).normalize(min, max, 0.0, 255.0)

        val shaped =
          if(rescaled.cols != 6000 || rescaled.rows != 6000) {
            rescaled.resample(gt.raster.extent, 6000, 6000)
          } else {
            rescaled
          }

        GeoTiff(Raster(shaped, gt.raster.extent), gt.crs).write(s"""${b3}normalized-byte-${new java.io.File(path).getName.replace("tff", "tif")}""")
        println(s"$path = ${shaped.dimensions}")
      }
    }
  }
}
