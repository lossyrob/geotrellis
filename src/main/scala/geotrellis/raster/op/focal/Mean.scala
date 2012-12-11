package geotrellis.raster.op.focal

import scala.math._

import geotrellis._
import geotrellis.raster._

case class Mean(r:Op[Raster],n:Op[Neighborhood]) extends FocalOp[Raster](r,n)({
  (r,n) =>
    n match {
      case Square(ext) => new CellwiseMeanCalc
      case _ => new CursorMeanCalc
    }
})

case class CellwiseMeanCalc() extends CellwiseCalculation with DoubleRasterDataResult {
  var count:Int = 0
  var sum:Double = 0.0

 def add(r:Raster, x:Int, y:Int) = {
    val z = r.get(x,y)
    if (z != NODATA) {
      count += 1
      sum   += z
    }
  }

  def remove(r:Raster, x:Int, y:Int) = {
    val z = r.get(x,y)
    if (z != NODATA) {
      count -= 1
      sum -= z
    }
  } 

  def setValue(x:Int,y:Int) = { data.setDouble(x,y, sum / count) }
  def reset() = { count = 0 ; sum = 0 }
}

case class CursorMeanCalc() extends CursorCalculation with DoubleRasterDataResult {
  var count:Int = 0
  var sum:Double = 0.0

  def calc(r:Raster,c:Cursor) = {
    c.removedCells.foreach { (x,y) => 
      var v = r.get(x,y)
      if(v != Double.NaN) { count -= 1; sum -= v } 
    }
    c.addedCells.foreach { (x,y) => 
      var v = r.get(x,y)
      if(v != Double.NaN) { count += 1; sum += v } 
    }
    data.setDouble(c.focusX,c.focusY,sum / count)
  }
}
