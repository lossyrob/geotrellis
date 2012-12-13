package geotrellis.raster.op.focal

import geotrellis._

import scala.math._

import Angles._

class SurfacePoint() {
  var isNaN = false
  var `dz/dx` = Double.NaN
  var `dz/dy` = Double.NaN

  def aspect() = {
    var a = atan2(`dz/dy`, -`dz/dx`)

    if (`dz/dx` == 0 && `dz/dy` == 0)
    {
      /* Flat area */
      a = Double.NaN
    } 
    else
    {
      if (a < 0) { a += 2*Pi }
    }

    if (a == 2*Pi) { a = 0.0 }
    a
  }
  def slope(zFactor:Double):Double = atan(zFactor * sqrt(`dz/dx` * `dz/dx` + `dz/dy` * `dz/dy`))
  def slope():Double = slope(1.0)

  // Trig functions for slope and aspect.
  // Use these if you want to get the sine or cosine of the aspect or slope,
  // since they are a lot more performant than calling scala.math.sin or 
  // scala.math.cos
  def cosSlope = {
    val denom = sqrt(`dz/dx` * `dz/dx` + `dz/dy` * `dz/dy` + 1)
    if(denom == 0) Double.NaN else {
      1/denom
    }
  }
  def sinSlope = {
    val denom = sqrt(`dz/dx` * `dz/dx` + `dz/dy` * `dz/dy` + 1)
    if(denom == 0) Double.NaN else {
      sqrt(`dz/dx` * `dz/dx` + `dz/dy` * `dz/dy`) / denom
    }
  }
  def cosAspect = {
    if(`dz/dx` == 0) { if(`dz/dy` == 0) -1 else 0 } else {
      if(`dz/dy` == 0) {
        if(`dz/dx` < 0) 1 else -1
      } else {
        -`dz/dx` / sqrt(`dz/dy`*`dz/dy` + `dz/dx`*`dz/dx`)
      }
    }
  }
  def sinAspect = {
    if(`dz/dy` == 0) 0 else {
      if(`dz/dx` == 0) {
        if(`dz/dy` < 0) -1 else if(`dz/dy` > 0) 1 else 0
      } else {
        `dz/dy` / sqrt(`dz/dy`*`dz/dy` + `dz/dx`*`dz/dx`)
      }
    }
  }
}

trait SurfacePointCalculation[T] extends FocalCalculation[T] {
  var lastY = -1

  var cellWidth = 0.0
  var cellHeight = 0.0

  var west = new Array[Double](3)
  var base = new Array[Double](3)
  var east = new Array[Double](3)

  val s = new SurfacePoint

  def setValue(x:Int,y:Int,s:SurfacePoint):Unit

  def setValue(x:Int,y:Int):Unit = {
    calcSurface()
    setValue(x,y,s)
  }
  
  def moveRight() = {
    val tmp = west
    west = base
    base = east
    east = tmp
  }
  
  def calcSurface():Unit = {
    if(base(1) == NODATA) {
      s.`dz/dx` = Double.NaN
      s.`dz/dy` = Double.NaN
    }

    s.`dz/dx` = (east(0) + 2*east(1) + east(2) - west(0) - 2*west(1) - west(2)) / (8 * cellWidth)
    s.`dz/dy` = (west(2) + 2*base(2) + east(2) - west(0) - 2*base(0) - east(0)) / (8 * cellHeight)
  }

  /*
   * Executes a specific traversal strategy for SurfacePointCalculation.
   * The difference between this and ScanLine for CellwiseCalculation is that for edge cases,
   * the value at the focus is added in place of out-of-border neighborhood
   * values.
   *
   * Assumes a Square(1) neighborhood.
   *
   */
  def execute(r:Raster,n:Neighborhood) = {
    val cols = r.cols
    val rows = r.rows
    cellWidth = r.rasterExtent.cellwidth
    cellHeight = r.rasterExtent.cellheight

    if(cols < 3 || rows < 3) { sys.error("Raster is too small to get surface values") }

    var y = 1
    var x = 1
    var focalValue = r.getDouble(0,0)
    
    // Handle top row
    
    /// Top Left
    west(0) = focalValue
    west(1) = focalValue
    west(2) = focalValue
    base(0) = focalValue
    base(1) = focalValue
    base(2) = r.getDouble(0,1)
    east(0) = focalValue
    east(1) = r.getDouble(1,0)
    east(2) = r.getDouble(1,1)
    setValue(0,0)
    
    /// Top Middle
    while (x < cols-1) {
      moveRight()
      focalValue = r.getDouble(x,0)
      west(0) = focalValue
      base(0) = focalValue
      east(0) = focalValue
      east(1) = r.getDouble(x+1,0)
      east(2) = r.getDouble(x+1,1)
      setValue(x, 0)
      x += 1
    }

    /// Top Right
    moveRight()
    focalValue = r.getDouble(x,0)
    west(0) = focalValue
    base(0) = focalValue
    east(0) = focalValue
    east(1) = focalValue
    east(2) = focalValue
    setValue(x,0)
    
    // Handle middle rows
    while (y < rows-1) {
      focalValue = r.getDouble(0,y)
      // Middle Left
      west(0) = focalValue
      west(1) = focalValue
      west(2) = focalValue
      base(0) = r.getDouble(0,y-1)
      base(1) = focalValue
      base(2) = r.getDouble(0,y+1)
      east(0) = r.getDouble(1,y-1)
      east(1) = r.getDouble(1,y)
      east(2) = r.getDouble(1,y+1)
      setValue(0,y)
      
      /// Middle Middle (ha)
      x = 1
      while (x < cols-1) {
        moveRight()
        east(0) = r.getDouble(x+1,y-1)
        east(1) = r.getDouble(x+1,y)
        east(2) = r.getDouble(x+1,y+1)
        setValue(x, y)
        x += 1
      }

      /// Middle Right
      moveRight()
      focalValue = r.getDouble(x,y)
      east(0) = focalValue
      east(1) = focalValue
      east(2) = focalValue
      setValue(x,y)
      y += 1
    }

    // Handle bottom row

    /// Bottom Left
    focalValue = r.getDouble(0,y)
    west(0) = focalValue
    west(1) = focalValue
    west(2) = focalValue
    base(0) = r.getDouble(0,y-1)
    base(1) = focalValue
    base(2) = focalValue
    east(0) = r.getDouble(1,y-1)
    east(1) = r.getDouble(1,y)
    east(2) = focalValue
    setValue(0,y)
    
    /// Bottom Middle
    x = 1
    while (x < cols-1) {
      moveRight()
      focalValue = r.getDouble(x,y)
      east(0) = r.getDouble(x+1,y-1)
      east(1) = r.getDouble(x+1,y)
      east(2) = focalValue
      base(2) = focalValue
      west(2) = focalValue
      setValue(x, y)
      x += 1
    }

    /// Bottom Right
    moveRight()
    focalValue = r.getDouble(x,y)
    east(0) = focalValue
    east(1) = focalValue
    east(2) = focalValue
    base(2) = focalValue
    west(2) = focalValue
    setValue(x,y)
  }
}
