package geotrellis.raster.op.focal

import geotrellis._
import geotrellis.raster._
import geotrellis.feature._
import geotrellis.testutil._

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CostDistanceSpec extends FunSuite with TestServer {
  implicit def arrayIsRasterOp(a: Array[Int]): Op[Raster] = {
    val size = math.sqrt(a.length).toInt    

    val e = Extent(0,0,10*size,10*size)
    val re = RasterExtent(e, 10,10,size,size)
    val data = IntArrayRasterData(a, size, size)

    Literal(new Raster(data, re))
  }

  def asRaster(a: Array[Int], cols: Int, rows: Int): Op[Raster] = {
    val e = Extent(0,0,10*cols,10*rows)
    val re = RasterExtent(e, 10,10,cols,rows)
    val data = IntArrayRasterData(a, cols, rows)

    Literal(new Raster(data, re))
  }
  
  test("ESRI example") {
    val n = NODATA
    val N = Double.NaN

    // Example from ESRI
    // http://webhelp.esri.com/arcgisdesktop/9.2/index.cfm?TopicName=Cost_Distance_algorithm
    val costRaster = Array(
      1,3,4,4,3,2,
      4,6,2,3,7,6,
      5,8,7,5,6,6,
      1,4,5,n,5,1,
      4,7,5,n,2,6,
      1,2,2,1,3,4)

    val points = Seq(
      (1,0),
      (2,0),
      (2,1),
      (0,5))

    val cd = CostDistance(costRaster, points)

    val d = server.run(cd).data.asInstanceOf[DoubleArrayRasterData]

    val expected = Array(
      2.0, 0.0, 0.0, 4.0, 6.7, 9.2,
      4.5, 4.0, 0.0, 2.5, 7.5, 13.1,
      8.0, 7.1, 4.5, 4.9, 8.9, 12.7,
      5.0, 7.5, 10.5,  N, 10.6, 9.2,
      2.5, 5.7, 6.4,   N, 7.1, 11.1,
      0.0, 1.5, 3.5, 5.0, 7.0, 10.5).map(i => " %04.1f " format i)

    val strings = d.array.map(i => " %04.1f " format i)

    strings should equal (expected)
  }

  test("GRASS example") {

    // Example from GRASS
    // http://grass.osgeo.org/grass64/manuals/r.cost.html
    val costRaster = asRaster(Array(
       2 , 2 , 1 , 1 , 5 , 5 , 5 ,
       2 , 2 , 8 , 8 , 5 , 2 , 1 ,
       7 , 1 , 1 , 8 , 2 , 2 , 2 ,
       8 , 7 , 8 , 8 , 8 , 8 , 5 ,
       8 , 8 , 1 , 1 , 5 , 3 , 9 ,
       8 , 1 , 1 , 2 , 5 , 3 , 9), 7, 6)

    val points = Seq((5,4))

    val cd = CostDistance(costRaster, points)

    val d = server.run(cd).data.asInstanceOf[DoubleArrayRasterData]
    
    val expected = Array(
      22,21,21,20,17,15,14,
      20,19,22,20,15,12,11,
      22,18,17,18,13,11, 9,
      21,14,13,12, 8, 6, 6,
      16,13, 8, 7, 4, 0, 6,
      14, 9, 8, 9, 6, 3, 8)

    d.array.map(i => (i + 0.5).toInt) should equal(expected)

  }

  def print(d: DoubleArrayRasterData):Unit = println(d.array.toList.map(i => " %04.1f " format i).grouped(d.cols).map(_.mkString(",")).mkString("\n"))

}
