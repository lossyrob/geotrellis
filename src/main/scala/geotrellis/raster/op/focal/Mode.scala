package geotrellis.raster.op.focal

import geotrellis._
import geotrellis.raster._
import geotrellis.statistics.FastMapHistogram

/** Computes the mode of a neighborhood for a given raster 
 *
 * @param    r      Raster on which to run the focal operation.
 * @param    n      Neighborhood to use for this operation (e.g., [[Square]](1))
 * @param    tns    TileNeighbors that describe the neighboring tiles.
 * @note            Mode does not currently support Double raster data.
 *                  If you use a Raster with a Double RasterType (TypeFloat,TypeDouble)
 *                  the data values will be rounded to integers.
 */
case class Mode(r:Op[Raster],n:Op[Neighborhood],tns:Op[TileNeighbors]) extends FocalOp[Raster](r,n,tns)({
  (r,n) => 
    n match {
      case Square(ext) => new CellwiseModeCalc(ext)
      case _ => new CursorModeCalc(n.extent)
    }
})

object Mode {
  def apply(r:Op[Raster],n:Op[Neighborhood]) = new Mode(r,n,TileNeighbors.NONE)
}

class CursorModeCalc(extent:Int) extends CursorCalculation[Raster] with IntRasterDataResult 
                                                                   with MedianModeCalculation {
  initArray(extent)
                                                         
  def calc(r:Raster,cursor:Cursor) = {
    cursor.removedCells.foreach { (x,y) =>
      val v = r.get(x,y)
      if(!v.isNoData) {
        removeValue(v)
      }
    }
    cursor.addedCells.foreach { (x,y) =>
      val v = r.get(x,y)
      if(!v.isNoData) addValue(v)
    }
    data.set(cursor.col,cursor.row,mode)
  }
}

class CellwiseModeCalc(extent:Int) extends CellwiseCalculation[Raster] with IntRasterDataResult 
                                                                       with MedianModeCalculation {
  initArray(extent)

  def add(r:Raster, x:Int, y:Int) = {
    val v = r.get(x,y)
    if (!v.isNoData) {
      addValue(v)
    }
  }

  def remove(r:Raster, x:Int, y:Int) = {
    val v = r.get(x,y)
    if (!v.isNoData) {
      removeValue(v)
    }
  } 

  def setValue(x:Int,y:Int) = { data.set(x,y,mode) }
}
