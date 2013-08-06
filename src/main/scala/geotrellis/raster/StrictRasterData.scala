package geotrellis.raster

import geotrellis._

/**
 * StrictRasterData is an ArrayRasterData which has already allocated its
 * values and which evaluates eagerly.
 *
 * This trait provides concrete, eager implementations of map, mapIfSet,
 * foreach, and combine.
 */
trait StrictRasterData extends ArrayRasterData with Serializable {
  def force = Option(this)

  def foreach(f:Int => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(apply(i))
      i += 1
    }
  }

  def map(f:Int => Int):ArrayRasterData = {
    val len = length
    val data = alloc(cols, rows)
    var i = 0
    while (i < len) {
      data(i) = f(apply(i))
      i += 1
    }
    data
  }

  def mapIfSet(f:Int => Int):ArrayRasterData = {
    val len = length
    val data = alloc(cols, rows)
    var i = 0
    while (i < len) {
      val z = apply(i)
      if (isData(z)) data(i) = f(z)
      i += 1
    }
    data
  }

  def combine(rhs:RasterData)(f:(Int, Int) => Int):RasterData = rhs match {
    case other:ArrayRasterData => {
      if (lengthLong != other.lengthLong) {
        val size1 = s"${cols} x ${rows}"
        val size2 = s"${other.cols} x ${other.rows}"
        sys.error(s"Cannot combine rasters of different sizes: $size1 vs $size2")
      }
      val output = RasterData.largestAlloc(this, other, cols, rows)
      var i = 0
      val len = length
      while (i < len) {
        output(i) = f(apply(i), other(i))
        i += 1
      }
      output
    }
    case _ => rhs.combine(this)((b, a) => f(a, b))
  }

  def foreachDouble(f:Double => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(applyDouble(i))
      i += 1
    }
  }


  def mapDouble(f:Double => Double):ArrayRasterData = {
    val len = length
    val data = alloc(cols, rows)
    var i = 0
    while (i < len) {
      data.updateDouble(i, f(applyDouble(i)))
      i += 1
    }
    data
  }

  def mapIfSetDouble(f:Double => Double):ArrayRasterData = {
    val len = length
    val data = alloc(cols, rows)
    var i = 0
    while (i < len) {
      val z = applyDouble(i)
      if (isData(z)) data.updateDouble(i, f(z))
      i += 1
    }
    data
  }

  def combineDouble(rhs:RasterData)(f:(Double, Double) => Double) = rhs match {
    case other:ArrayRasterData => {
      if (lengthLong != other.lengthLong) {
        val size1 = s"${cols} x ${rows}"
        val size2 = s"${other.cols} x ${other.rows}"
        sys.error(s"Cannot combine rasters of different sizes: $size1 vs $size2")
      }
      val output = RasterData.largestAlloc(this, other, cols, rows)
      var i = 0
      val len = length
      while (i < len) {
        output.updateDouble(i, f(applyDouble(i), other.applyDouble(i)))
        i += 1
      }
      output
    }
    case _ => rhs.combineDouble(this)((b, a) => f(a, b))
  }
}
