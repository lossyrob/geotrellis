package geotrellis.testutil

import geotrellis._
import geotrellis.raster.op._
import geotrellis.process._

import org.scalatest.{BeforeAndAfter,Suite}
import org.scalatest.matchers._

object TestServer {
  lazy val server:Server = new Server("testutil", Catalog.fromPath("src/test/resources/catalog.json"))
}
/*
 * Mixin to provide a server that is reset for each test
 */
trait TestServer extends Suite with BeforeAndAfter with ShouldMatchers {
  val server = TestServer.server 

  def run[T:Manifest](op:Op[T]):T = server.run(op)
  def getResult[T:Manifest](op:Op[T]) = server.getResult(op)

  def get(name:String) = io.LoadRaster(name)

  def assertEqual(r:Op[Raster],arr:Array[Int]) = {
    run(r).toArray should equal (arr)
  }

  def assertEqual(rd:RasterData,arr:Array[Int]) = {
    (rd.cols * rd.rows) should be (arr.length)
    for(col <- 0 until rd.cols) {
      for(row <- 0 until rd.rows) {
        rd.get(col,row) should be (arr(row*rd.cols + col))
      }
    }
  }

  def assertEqual(r:Op[Raster],r2:Op[Raster]):Unit = assertEqual(r,r2,0.0000000001)

  def assertEqual(rOp1:Op[Raster],rOp2:Op[Raster],threshold:Double):Unit = {
    val r1 = run(rOp1)
    val r2 = run(rOp2)
    
    r1.rasterExtent should be (r2.rasterExtent)

    r1.cols should be (r2.cols)
    r1.rows should be (r2.rows)
    r1.isFloat should be (r2.isFloat)

    val isFloat = r1.isFloat
    for(col <- 0 until r1.cols) {
      for(row <- 0 until r1.rows) {
        if(isFloat) {
          if(math.abs(r1.getDouble(col,row) - r2.getDouble(col,row)) >= threshold) {
            println(s"Failure at (${col},${row})")
            r1.getDouble(col,row) should be (r2.getDouble(col,row))
          }
        } else {
          if(r1.get(col,row) != (r2.get(col,row)))
            println(s"Failure at (${col},${row})")
          r1.get(col,row) should be (r2.get(col,row))
        }
      }
    }
  }
}


