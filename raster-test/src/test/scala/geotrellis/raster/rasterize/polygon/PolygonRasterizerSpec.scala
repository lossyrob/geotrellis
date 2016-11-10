/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.rasterize.polygon

import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.Rasterizer.Options
import geotrellis.raster.testkit._
import geotrellis.vector._

import math.{max,min,round}

import com.vividsolutions.jts.{geom => jts}
import com.vividsolutions.jts.io.WKTReader
import org.scalatest.FunSuite
import scala.collection.mutable


class PolygonRasterizerSpec extends FunSuite
    with RasterMatchers
    with TileBuilders {

  test("Polygon Rasterization") {
    val e = Extent(0.0, 0.0, 10.0, 10.0)
    val re = RasterExtent(e, 1.0, 1.0, 10, 10)

    val square  = Polygon( Line((1,9), (1,6), (4,6), (4,9), (1,9)) )
    val diamond = Polygon( Line((3,7), (6,4), (3,1), (0,4), (3,7)))
    val triangle = Polygon( Line((2,8),(5,5),(6,7), (6,7), (2,8)))

    val outsideSquare = Polygon( Line((51,59), (51,56), (54,56), (54,59), (51,59)) )
    val envelopingSquare = Extent(0.0, 0.0, 10.0, 10.0).toPolygon

    // intersection on cell midpoint
    val square2 = Polygon( Line( (1.0,9.0), (1.0,8.5), (1.0,6.0), (4.0, 6.0), (4.0, 8.5), (4.0, 9.0), (1.0, 9.0) ))

    val r1 = Rasterizer.rasterizeWithValue(square, re, 0x11)
    // println(r1.tile.asciiDraw)

    // values match gdal
    for ( i <- 1 to 3; j <- 1 to 3) {
      assert(r1.get(i,j) === 0x11)
    }

    var sum = 0
    r1.foreach(f => if (isData(f)) sum = sum + 1 )
    assert(sum === 9)

    val r2 = Rasterizer.rasterizeWithValue(diamond, re, 0x22)
    // println(r2.tile.asciiDraw())
    assert(r2.get(3,3) === 0x22)
    for (i <- 2 to 4) { assert(r2.get(i,4) === 0x22) }
    for (i <- 1 to 5) { assert(r2.get(i,5) === 0x22) }
    for (i <- 1 to 5) { assert(r2.get(i,6) === 0x22) }
    for (i <- 2 to 4) { assert(r2.get(i,7) === 0x22) }
    assert(r2.get(3,8) === 0x22)

    sum = 0
    r2.foreach(f => if (isData(f)) sum = sum + 1 )
    assert(sum === 18)

    val r3 = Rasterizer.rasterizeWithValue(triangle, re, 0x33)
    // println(r3.tile.asciiDraw())

    assert(r3.get(3,2) === 0x33)
    assert(r3.get(4,3) === 0x33)
    assert(r3.get(5,3) === 0x33)
    sum = 0
    r3.foreach(f => if (isData(f)) sum = sum + 1 )
    assert(sum === 3)

    val r4 = Rasterizer.rasterizeWithValue(square2, re, 0x44)
    // println(r4.tile.asciiDraw())

    val r5 = Rasterizer.rasterizeWithValue(outsideSquare, re, 0x55)
    // println(r5.tile.asciiDraw())

    val r6 = Rasterizer.rasterizeWithValue(envelopingSquare, re, 0x66)
    // println(r6.tile.asciiDraw())

    val emptyGeom = outsideSquare.intersection(envelopingSquare)
    // LoadWKT()
  }

  test("failing example should work") {
    val p = Polygon(Line((-9510600.807354769, 4176519.1962707597), (-9511212.30358105,4172238.854275199), (-9503568.600752532,4175602.1747499597), (-9510600.807354769,4176519.1962707597)))
    val re = RasterExtent(Extent(-9509377.814902207,4174073.2405969054,-9508766.318675926,4174684.736823185),2.3886571339098737,2.3886571339044167,256,256)
    val r = Rasterizer.rasterizeWithValue(p, re, 1 )
    var sum = 0
    r.foreach(v => if (isData(v)) sum = sum + 1)
    assert(sum === 65536)
  }

  test("polygon rasterization: more complex polygons") {
    val p1 = Polygon (Line((-74.6229572569999, 41.5930024740001),
      (-74.6249086829999, 41.5854607480001),
      (-74.6087045219999, 41.572877582),
      (-74.6396698609999, 41.5479203780001),
      (-74.6134071899999, 41.5304959030001),
      (-74.6248611209999, 41.5210940920001),
      (-74.6080037309999, 41.510192955),
      (-74.61917188, 41.500007054),
      (-74.6868377089999, 41.5507426980001),
      (-74.6752089579999, 41.5646628200001),
      (-74.6776005779999, 41.573316585),
      (-74.6637320329999, 41.5691605160001),
      (-74.6623717069999, 41.5770289280001),
      (-74.6558314389999, 41.552671724),
      (-74.6494842519999, 41.5467347190001),
      (-74.6459184919999, 41.565179846),
      (-74.6344289929999, 41.5694043560001),
      (-74.6229572569999, 41.5930024740001)))

    val tileExtent = Extent( -88.57589314970001, 35.15178531379998, -70.29017892250002, 53.43749954099997)
    // val rasterExtent = RasterExtent(tileExtent, 0.008929,0.008929, 2048, 2048 )
    // val rasterExtent = RasterExtent(tileExtent, 0.008929,0.008929, 2048, 2048 )

    val cw1024 = 0.0178571428
    val cw = cw1024 * 2
    val rasterExtent = RasterExtent(tileExtent, cw, cw, 512, 512)
    // at 512, cells are:
    // 389, 332
    // 390, 332
    // 390, 333

    val r1 = p1.rasterizeWithValue(rasterExtent, 0x55)
    var sum = 0
    r1.foreach(f => if (isData(f)) sum = sum + 1 )
    assert(sum === 3)
    assert(r1.get(389,332) === 0x55)
    assert(r1.get(390,332) === 0x55)
    assert(r1.get(390,333) === 0x55)
    // println("sum: " + sum)
  }

  test("Rasterization tests from directory of WKT files") {
    // This test loads WKT text files from raster-test/data/feature which are
    // named xxx_nnn.wkt, where nnn is the number of cells we expect rasterization
    // to determine to be within the defined feature in the raster extent defined
    // below.

    // To create a new test, create a .wkt and a GeoJSON file of the polygon
    // in the directory, with the same name (except for extension).
    // For example, one quick way to do this: http://openlayers.org/dev/examples/vector-formats.html

    // Run the gdaltest.sh script with the name of the two files (without extension).
    // The final line is the number of cells included by GDAL (center of cell mode).
    // Rename the .wkt file to end with _N (where N is the number).

    def countRasterizedCells( wktFile:java.io.File, re:RasterExtent ) = {
      val json = scala.io.Source.fromFile(wktFile).mkString
      val filename = wktFile.getName()
      //      println("Testing rasterization: " + filename)
      val g1 = new WKTReader().read(json).asInstanceOf[jts.Polygon]

      if (g1.isValid){
        val count =
        Integer.parseInt(wktFile.getName()
          .subSequence(0, filename.length - 4)
          .toString
          .split("_")
          .last)

        val p1 = Polygon(g1)
        var sum = 0
        val re = RasterExtent( Extent(0, 0, 300, 300), 1, 1, 300, 300)
        val r = PolygonRasterizer.foreachCellByPolygon(p1, re) { (x:Int, y:Int) =>
          sum = sum + 1
        }

        (sum, count)
      }else{
        (0,0)
      }

    }

    val f = new java.io.File("raster-test/data/vector/")
    val fs = f.listFiles.filter(_.getPath().endsWith(".wkt"))

    val re = RasterExtent( Extent(0, 0, 300, 300), 1, 1, 300, 300)
    fs.foreach( f => {
      val (sum, count) = countRasterizedCells(f, re)
      assert ( sum === count )
    })
  }

  test("Diamond polygon w/ point pixels, scanline through corner") {
    val extent = Extent(0, 0, 11, 11)
    val rasterExtent = RasterExtent(extent, 11, 11)
    val diamond = Polygon(List( Point(0,5.5), Point(5.5,11), Point(11,5.5), Point(0,5.5) ))

    val s = mutable.Set[(Int, Int)]()
    PolygonRasterizer.foreachCellByPolygon(diamond, rasterExtent) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (36)
  }

  test("Flipped-diamond polygon w/ point pixels, scanline through corner") {
    val extent = Extent(0, 0, 11, 11)
    val rasterExtent = RasterExtent(extent, 11, 11)
    val diamond = Polygon(List( Point(5.5,0), Point(11,5.5), Point(5.5,11), Point(5.5,0) ))

    val s = mutable.Set[(Int, Int)]()
    PolygonRasterizer.foreachCellByPolygon(diamond, rasterExtent) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (25)
  }

  test("Polygon w/ non-point pixels, w/o partial cells, not contianing border center cells") {
    val extent = Extent(0, 0, 10, 10)
    val rasterExtent = RasterExtent(extent, 10, 10)
    val extent2 = Extent(0.7, 0.7, 9.3, 9.3)

    val s = mutable.Set[(Int, Int)]()
    PolygonRasterizer.foreachCellByPolygon(extent2, rasterExtent) { (col, row) =>
      s += ((col, row))
    }

    assert(s.size < 100)
  }

  test("Polygon w/ non-point pixels and partial cells, not contianing border center cells") {
    val extent = Extent(0, 0, 10, 10)
    val rasterExtent = RasterExtent(extent, 10, 10)
    val extent2 = Extent(0.7, 0.7, 9.3, 9.3)
    val options = Options(includePartial = true, sampleType = PixelIsArea)

    val s = mutable.Set[(Int, Int)]()
    PolygonRasterizer.foreachCellByPolygon(extent2, rasterExtent, options) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (100)
  }

  test("Polygon w/ non-point pixels and w/ partial cells") {
    val p = Polygon(Line((0.0,0.0), (0.0,1.0), (0.5,1.5), (0.0,2.0), (0.0,3.0), (3.0,3.0), (3.0,0.0), (0.0,0.0)))
    val rasterExtent = RasterExtent(Extent(0, 0, 3, 3), 3, 3)
    val options = Options(includePartial = true, sampleType = PixelIsArea)
    val s = mutable.Set.empty[(Int, Int)]

    PolygonRasterizer.foreachCellByPolygon(p, rasterExtent, options) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (9)
  }

  test("Polygon w/ non-point pixels and w/o partial cells") {
    val p = Polygon(Line((0.0,0.0), (0.0,1.0), (0.5,1.5), (0.0,2.0), (0.0,3.0), (3.0,3.0), (3.0,0.0), (0.0,0.0)))
    val rasterExtent = RasterExtent(Extent(0, 0, 3, 3), 3, 3)
    val options = Options(includePartial = false, sampleType = PixelIsArea)
    val s = mutable.Set[(Int, Int)]()

    PolygonRasterizer.foreachCellByPolygon(p, rasterExtent, options) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (8)
  }

  test("Smaller polygon w/ non-point pixels and w/o partial cells") {
    val p = Polygon(Line((0.01,0.01), (0.01,1.0), (0.5,1.5), (0.01,2.0), (0.01,2.99), (2.99,2.99), (2.99,0.01), (0.01,0.01)))
    val rasterExtent = RasterExtent(Extent(0, 0, 3, 3), 3, 3)
    val options = Options(includePartial = false, sampleType = PixelIsArea)
    val s = mutable.Set[(Int, Int)]()

    PolygonRasterizer.foreachCellByPolygon(p, rasterExtent, options) { (col, row) =>
      s += ((col, row))
    }

    s.size should be (1)
  }

  test("Rasterization of a polygon with a hole in it") {
    val p = Polygon(
      Line( (0,0), (4, 0), (4, 4), (0, 4), (0, 0) ),
      Line( (1, 1), (3, 1), (3, 3), (1, 3), (1, 1) )
    )

    val re = RasterExtent(Extent(-1, -1, 5, 5), 6, 6)

    val tile = IntArrayTile.empty(6, 6)

    var sum = 0
    PolygonRasterizer.foreachCellByPolygon(p, re) { (col: Int, row: Int) =>
      tile.set(col, row, 1)
      sum = sum + 1
    }
    // println(tile.tile.asciiDraw)

    sum should be (12)
  }

  val triangle2 = Polygon(Point(0, 0), Point(0, 100), Point(100, 50), Point(0, 0))

  test("Triangle w/ point pixels") {
    /*
     1    ND    ND    ND    ND    ND    ND    ND    ND    ND
     1     1     1    ND    ND    ND    ND    ND    ND    ND
     1     1     1     1     1    ND    ND    ND    ND    ND
     1     1     1     1     1     1     1    ND    ND    ND
     1     1     1     1     1     1     1     1     1    ND
     1     1     1     1     1     1     1     1     1    ND
     1     1     1     1     1     1     1    ND    ND    ND
     1     1     1     1     1    ND    ND    ND    ND    ND
     1     1     1    ND    ND    ND    ND    ND    ND    ND
     1    ND    ND    ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 10, 10)
    val ro = Options(includePartial = true, sampleType = PixelIsPoint)
    val tile = IntArrayTile.empty(10, 10)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(triangle2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (50)
//    println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("Triangle w/ non-point pixels and w/o partial cells") {
    /*
     ND    ND    ND    ND    ND    ND    ND    ND    ND    ND
      1     1    ND    ND    ND    ND    ND    ND    ND    ND
      1     1     1     1    ND    ND    ND    ND    ND    ND
      1     1     1     1     1     1    ND    ND    ND    ND
      1     1     1     1     1     1     1     1    ND    ND
      1     1     1     1     1     1     1     1    ND    ND
      1     1     1     1     1     1    ND    ND    ND    ND
      1     1     1     1    ND    ND    ND    ND    ND    ND
      1     1    ND    ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 10, 10)
    val ro = Options(includePartial = false, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(10, 10)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(triangle2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (40)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("Triangle w/ non-point pixels and w/ partial cells") {
    /*
     1     1    ND    ND    ND    ND    ND    ND    ND    ND
     1     1     1     1    ND    ND    ND    ND    ND    ND
     1     1     1     1     1     1    ND    ND    ND    ND
     1     1     1     1     1     1     1     1    ND    ND
     1     1     1     1     1     1     1     1     1     1
     1     1     1     1     1     1     1     1     1     1
     1     1     1     1     1     1     1     1    ND    ND
     1     1     1     1     1     1    ND    ND    ND    ND
     1     1     1     1    ND    ND    ND    ND    ND    ND
     1     1    ND    ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 10, 10)
    val ro = Options(includePartial = true, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(10, 10)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(triangle2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (60)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  val tiny = Polygon(Point(40, 40), Point(40, 59), Point(59, 50), Point(40, 40))

  test("Sub-Pixel Geometry w/ point pixels") {
    /*
     ND    ND    ND
     ND     1    ND
     ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 3, 3)
    val ro = Options(includePartial = true, sampleType = PixelIsPoint)
    val tile = IntArrayTile.empty(3, 3)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (1)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("Sub-Pixel Geometry w/ non-point pixels and w/ partial cells") {
    /*
     ND    ND    ND
     ND     1    ND
     ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 3, 3)
    val ro = Options(includePartial = true, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(3, 3)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (1)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("Sub-Pixel Geometry w/ non-point pixels and w/o partial cells") {
    /*
     ND    ND    ND
     ND    ND    ND
     ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 3, 3)
    val ro = Options(includePartial = false, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(3, 3)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (0)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  val tiny2 = Polygon(Point(40, 40), Point(40, 42), Point(42, 41), Point(40, 40))

  test("More Sub-Pixel Geometry w/ point pixels") {
    /*
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 7, 7)
    val ro = Options(includePartial = true, sampleType = PixelIsPoint)
    val tile = IntArrayTile.empty(7, 7)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (0)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("More Sub-Pixel Geometry w/ non-point pixels and w/ partial cells") {
    /*
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND     1    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 7, 7)
    val ro = Options(includePartial = true, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(7, 7)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (1)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }

  test("More Sub-Pixel Geometry w/ non-point pixels and w/o partial cells") {
    /*
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     ND    ND    ND    ND    ND    ND    ND
     */
    val e = Extent(0, 0, 100, 100)
    val re = RasterExtent(e, 7, 7)
    val ro = Options(includePartial = false, sampleType = PixelIsArea)
    val tile = IntArrayTile.empty(7, 7)
    var count = 0

    PolygonRasterizer.foreachCellByPolygon(tiny2, re, ro)({ (col, row) =>
      tile.set(col, row, 1)
      count += 1
    })

    count should be (0)
    // println(GeoTiff(tile, e, LatLng).tile.asciiDraw)
  }
}
