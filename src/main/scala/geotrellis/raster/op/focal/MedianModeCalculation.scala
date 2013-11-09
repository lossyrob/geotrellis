package geotrellis.raster.op.focal

import geotrellis._

trait MedianModeCalculation {
  val modeValue = 0
  var currArrMax = 0

  var arr:Array[Int] = null
  var d2:Int = 0

  def initArray(extent:Int) = {
    val d = extent*2 + 1
    d2 = d*d
    arr = Array.ofDim[Int](d2)
    for(i <- 0 until arr.length) { arr(i) = NODATA }
  }

  def reset() = { 
    var i = 0
    while(i < d2) { arr(i) = NODATA ; i += 1 }
    currArrMax = 0
  }

  /** Adds a value to the internal array,
   * ordering the values. If you call addValue
   * after calling this you'll probably break the
   * ordering, so use one or the other. This one
   * is slower, according to timing data.
   */
 def addValueOrdered(v:Int) = {
    var i = 0
    var arrV = 0
    var break = false
    while(!break)
    {
      arrV = arr(i)
      if(arrV.isNoData) {
        arr(i) = v
        currArrMax += 1
        break = true
      } else if(arrV > v) {
        var j = currArrMax
        while(j > i) {
          arr(j) = arr(j-1)
          j -= 1
        }
        arr(i) = v
        currArrMax += 1
        break = true
      }
      i += 1
    }
  }

  /** Adds a value to the internal array,
   * grouping values together but not ordering
   * them. If you call this after calling
   * addValueOrdered, you'll probably break the
   * ordering, so use one or the other. This one
   * is faster, according to timing data.
   */
  def addValue(v:Int) = {
    var i = 0
    var break = false
    while(!break)
    {
      if(arr(i).isNoData) {
        arr(i) = v
        currArrMax += 1
        break = true
      }
      else if(arr(i) == v) {
        var j = currArrMax
        while(j > i) {
          arr(j) = arr(j-1)
          j -= 1
        }
        currArrMax += 1
        break = true
      }
      i += 1
    }
  }

  def removeValue(v:Int) = {
    var i = 0
    var break = false
    while(arr(i) != v && i < d2-1) i += 1
    if(i == d2-1) { 
      arr(i) = NODATA 
      currArrMax -= 1
    } else {
      while(!break) {
        arr(i) = arr(i+1)
        if(i == d2-2) {           
          arr(d2-1) = NODATA
          currArrMax -= 1
          break = true
        }
        i += 1
      }
    }
  }

  /** Calculates the median. If you use this,
   *  make sure you've been only calling addValueOrdered
   */
  def median:Int = {
    if(currArrMax == 0) NODATA else {
      if(currArrMax % 2 == 0) {
       (arr(currArrMax/2) + arr(currArrMax/2-1)) / 2
      } else {
        arr(currArrMax/2)
      }
    }
  }

  /** Calculates the mode. 
   * Works with addValue or addValueOrdered
   */
  def mode = {
    var i = 1
    var m = arr(0)
    var modeCount = 1
    var curValue = m
    var curCount = 1

    var break = false
    while(!break) {
      val v = arr(i)
      if(v.isNoData) {
        if(curCount > modeCount) {
          m = curValue
          modeCount = curCount
        }
        
        break = true
      }
        else {
          if(v == curValue) {
            curCount += 1
          } else {
            if(curCount > modeCount) {
              m = curValue
              modeCount = curCount
            }
            curValue = v
            curCount = 1
          }
          i += 1
          if(i == d2) break = true
        }
    }

    m
  }
}
