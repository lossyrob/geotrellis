package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import geotrellis.statistics._

trait ZonalSummaryOpMethods[+Repr <: RasterSource] { self:Repr =>
  def zonalHistogram[D](p:Op[feature.Polygon[D]]):ValueSource[Histogram] = 
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          val histogram = FastMapHistogram()
          r.foreach((z:Int) => if (!z.isNoData) histogram.countItem(z, 1))
          histogram
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          val histogram = FastMapHistogram()
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply (col:Int, row:Int, g:Geometry[D]) {
                  val z = r.get(col,row)
                  if (!z.isNoData) histogram.countItem(z, 1)
                }
              }
            )
          }

          histogram
      }
    }.converge

  def zonalSum[D](p:Op[feature.Polygon[D]]):ValueSource[Long] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var s = 0L
          r.foreach((x:Int) => if (!x.isNoData) s = s + x)
          s
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var sum: Long = 0L
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.get(col,row)
                  if (!z.isNoData) { sum = sum + z }
                }
              }
            )
          }

          sum
      }
    }.reduce(_+_)

  def zonalSumDouble[D](p:Op[feature.Polygon[D]]):ValueSource[Double] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var s = 0.0
          r.foreachDouble((x:Double) => if (!x.isNoData) s = s + x)
          s
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var sum = 0.0
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.getDouble(col,row)
                  if(!z.isNoData) { sum = sum + z }
                }
              }
            )
          }

          sum
      }
    }.reduce(_+_)

  def zonalMin[D](p:Op[feature.Polygon[D]]):ValueSource[Int] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var min = NODATA.int
          r.foreach { (x:Int) => 
            if (!x.isNoData && (x < min || min.isNoData)) { min = x }
          }
          min
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var min = NODATA.int
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.get(col,row)
                  if (!z.isNoData && (z < min || min.isNoData) ) { min = z }
                }
              }
            )
          }
          min
      }
    }.reduce { (a,b) => 
      if(a.isNoData) { b } 
      else if(b.isNoData) { a }
      else { math.min(a,b) }
    }

  def zonalMinDouble[D](p:Op[feature.Polygon[D]]):ValueSource[Double] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var min = Double.NaN
          r.foreach((x:Int) => if (!x.isNoData && (x < min || min.isNoData)) { min = x })
          min
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var min = Double.NaN
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.getDouble(col,row)
                  if (!z.isNoData && (z < min || min.isNoData)) { min = z }
                }
              }
            )
          }

          min
      }
    }.reduce { (a,b) => 
      if(a.isNoData) { b } 
      else if(b.isNoData) { a }
      else { math.min(a,b) }
    }

  def zonalMax[D](p:Op[feature.Polygon[D]]):ValueSource[Int] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var max = NODATA.int // == Int.MinValue, so no need to check max.isNoData
          r.foreach((x:Int) => if (!x.isNoData && x > max) { max = x })
          max
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var max = NODATA.int
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.get(col,row)
                  if (!z.isNoData && z > max) { max = z }
                }
              }
            )
          }
          max
      }
    }.reduce { (a,b) => 
      if(a.isNoData) { b } 
      else if(b.isNoData) { a }
      else { math.max(a,b) }
    }

  def zonalMaxDouble[D](p:Op[feature.Polygon[D]]):ValueSource[Double] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var max = Double.NaN
          r.foreach((x:Int) => if (!x.isNoData && (x > max || max.isNoData)) { max = x })
          max
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var max = Double.NaN
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.getDouble(col,row)
                  if (!z.isNoData && (z > max || max.isNoData)) { max = z }
                }
              }
            )
          }
          max
      }
    }.reduce { (a,b) => 
      if(a.isNoData) { b } 
      else if(b.isNoData) { a }
      else { math.max(a,b) }
    }

  def zonalMean[D](p:Op[feature.Polygon[D]]):ValueSource[Double] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var s = 0L
          var c = 0L
          r.foreach((x:Int) => if (!x.isNoData) { s = s + x; c = c + 1 })
          Mean(s,c)
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var sum: Long = 0L
          var count: Int = 0
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.get(col,row)
                  if (!z.isNoData) { sum = sum + z; count = count + 1 }
                }
              }
            )
          }

          Mean(sum,count)
      }
    }.reduce(_+_).map(_.mean)

  def zonalMeanDouble[D](p:Op[feature.Polygon[D]]):ValueSource[Double] =
    self.mapIntersecting(p) { tileIntersection =>
      tileIntersection match {
        case FullTileIntersection(r:Raster) =>
          var s = 0.0
          var c = 0L
          r.foreachDouble((x:Double) => if (!x.isNoData) { s = s + x; c = c + 1 })
          Mean(s,c)
        case PartialTileIntersection(r:Raster,polygons:List[_]) =>
          var sum = 0.0
          var count = 0L
          for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
            Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
              new Callback[Geometry,D] {
                def apply(col:Int, row:Int, g:Geometry[D]) {
                  val z = r.getDouble(col,row)
                  if (!z.isNoData) { sum = sum + z; count = count + 1 }
                }
              }
            )
          }
          Mean(sum,count)
      }
    }.reduce(_+_).map(_.mean)
}

case class Mean(sum: Double, count: Long) {
  def mean:Double = if (count == 0) {
    Double.NaN
  } else {
    sum/count
  }
  def +(b: Mean) = Mean(sum + b.sum,count + b.count)
}
