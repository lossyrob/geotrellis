package geotrellis.raster.op.focal

import geotrellis._

case class Conway(r:Op[Raster]) extends FocalOp[Raster](r,Square(1))({
  (r,n) => new CellwiseCalculation[Raster] with ByteRasterDataResult {
    var count = 0

    def add(r:Raster, x:Int, y:Int) = {
      val z = r.get(x,y)
      if (z != NODATA) {
        count += 1
      }
    }

    def remove(r:Raster, x:Int, y:Int) = {
      val z = r.get(x,y)
      if (z != NODATA) {
        count -= 1
      }
    } 

    def setValue(x:Int,y:Int) = data.set(x,y, if(count == 3 || count == 2) 1 else NODATA)
    def reset() = { count = 0 }
  }
})
