package geotrellis.raster.op.focal

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import geotrellis._
import geotrellis.raster._

import scala.collection.mutable.Set

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class FocalStrategySpec extends FunSpec with ShouldMatchers {
  describe("CursorStrategy") {
    it("should execute the ZigZag traversal strategy correctly") {
      val rex = RasterExtent(Extent(0,0,5,5), 1,1,5,5)
      val d = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)
      val r = Raster(d,rex)
      val cur = Cursor(r,Square(1))
      
      var lastY = 0
      var lastX = -1

      val calc = new CursorCalculation {
        def calc(r:Raster,cursor:Cursor) = {
          if(cursor.focusY != 0 || cursor.focusX != 0 ) { cursor.isReset should equal(false) }
          if(lastY != cursor.focusY) {
            cursor.focusY should be > lastY
            cursor.focusX should equal(lastX)
            lastY = cursor.focusY
          } else {
            if(cursor.focusY % 2 == 0) { (cursor.focusX - lastX) should equal (1) }
            else { (cursor.focusX - lastX) should equal (-1) }
          }
          lastX = cursor.focusX
        }
      }

      CursorStrategy.execute(r,cur,calc, TraversalStrategy.ZigZag)
    }

    it("should execute the ScanLine traversal strategy correctly") {
      val rex = RasterExtent(Extent(0,0,5,5), 1,1,5,5)
      val d = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25)
      val r = Raster(d,rex)
      val cur = Cursor(r,Square(1))
      
      var lastY = -1
      var lastX = 0

      val calc = new CursorCalculation {
        def calc(r:Raster,cursor:Cursor) = {
          if(lastY != cursor.focusY) {
            cursor.isReset should equal(true)
            cursor.focusY should be > lastY
            cursor.focusX should equal(0)
            lastY = cursor.focusY
          } else {
            cursor.focusX should be  > lastX
          }
          lastX = cursor.focusX
        }
      }

      CursorStrategy.execute(r,cur,calc,TraversalStrategy.ScanLine)
    }
  }
}
