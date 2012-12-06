package geotrellis.raster.op.focal

import scala.math._

import geotrellis._
import geotrellis.raster._
import geotrellis.statistics._

case class Median(r:Op[Raster],n:Op[Neighborhood]) extends FocalOp[Raster](r,n)({
  (r,n) => new CursorCalculation with IntRasterDataResult {
    def calc(r:Raster,cursor:Cursor) = {
      val h = FastMapHistogram()
      cursor.allCells.foreach { (x,y) => h.countItem(r.get(x,y),1) }
      data.set(cursor.focusX,cursor.focusY,h.getMedian)
    }
  }
})
