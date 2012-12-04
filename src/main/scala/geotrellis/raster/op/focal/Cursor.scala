package geotrellis.raster.op.focal

import scala.collection.mutable
import scala.math.{min,max}

import geotrellis._

sealed trait Movement { val isVertical:Boolean }

object Movement { 
  val Up = new Movement { val isVertical = true }
  val Down = new Movement { val isVertical = true }
  val Left = new Movement { val isVertical = false }
  val Right = new Movement { val isVertical = false }
  val NoMovement = new Movement { val isVertical = false }
}
import Movement._

object Cursor {
  def getInt(r:Raster,n:Neighborhood) = {
    val cur = new IntCursor(r,n.extent)
    if(n.hasMask) { cur.setMask(n.mask) }
    cur
  }

  def getDouble(r:Raster,n:Neighborhood) = {
    val cur = new DoubleCursor(r,n.extent)
    if(n.hasMask) { cur.setMask(n.mask) }
    cur
  }
}

trait Cursor {
  def centerOn(x:Int,y:Int):Unit
  def move(m:Movement):Unit
}

trait IntCellSet {
  def foreach(cb:IntFocalValueCB):Unit
  def foreach(cb:IntFocalCellCB):Unit

  /*
   * Get all unmasked cell values covered by the cursor
   * in a sequence. (Non-performant)
   */
  def getAll:Seq[Int] = {
    val result = mutable.Set[Int]()
    foreach(new IntFocalValueCB { def act(v:Int) = { result += v } })
    result.toSeq
  }

  /*
   * Fold left along all the cell values of the raster
   * which are covered by the cursor and not masked.
   *
   * @param     seed      Seed for the fold operation.
   * @param     f         Function that takes in the seed, or previous computed value,
   *                      and computes a value to be passed into the next iteration.
   */
  def foldLeft(seed:Int)(cb:IntFocalFoldCB) = {
    var a = seed
    foreach(new IntFocalValueCB { def act(v:Int) = { a = cb.act(a,v) } })
    a
  }
}

trait DoubleCellSet {
  def foreach(cb:DoubleFocalValueCB):Unit
  def foreach(cb:DoubleFocalCellCB):Unit

  /*
   * Get all unmasked cell values covered by the cursor
   * in a sequence. (Non-performant)
   */
  protected def getAll:Seq[Double] = {
    val result = mutable.Set[Double]()
    foreach(new DoubleFocalValueCB { def act(v:Double) = { result += v } })
    result.toSeq
  }

  /*
   * Fold left along all the cell values of the raster
   * which are covered by the cursor and not masked.
   *
   * @param     seed      Seed for the fold operation.
   * @param     f         Function that takes in the seed, or previous computed value,
   *                      and computes a value to be passed into the next iteration.
   */
  protected def foldLeft(seed:Double)(cb:DoubleFocalFoldCB) = {
    var a = seed
    foreach(new DoubleFocalValueCB { def act(v:Double) = { a = cb.act(a,v) } })
    a
  }
}

/**
 * Represents a cursor that can be used to iterate over cells within a focal
 * neighborhood.
 *
 * @param      r                     Raster that this cursor runs over
 * @param      distanceFromCenter    The distance from the focus that the
 *                                   bounding box of this cursor extends.
 *                                   e.g. if the bounding box is 9x9, then
 *                                   the distance from center is 1.
 */
sealed abstract class BaseCursor(r:Raster, distanceFromCenter:Int) extends Cursor {
  protected val raster = r

  val dim = distanceFromCenter
  private val d = 2*dim + 1

  var mask:CursorMask = null
  private var hasMask = false

  // Values to track the bound of the cursor
  private var _xmin = 0
  private var _xmax = 0
  private var _ymin = 0
  private var _ymax = 0

  // Values to track added\removed values
  private var addedCol = 0
  private var removedCol = 0

  private var addedRow = 0
  private var removedRow = 0

  private var movement = NoMovement

  // Values to track the focus of the cursor
  private var _focusX = 0
  private var _focusY = 0

  def focusX = _focusX
  def focusY = _focusY

  def xmin = _xmin
  def xmax = _xmax
  def ymin = _ymin
  def ymax = _ymax

  def isReset = movement == NoMovement

  /*
   * Centers the cursor on a cell of the raster.
   * Added\Removed cells are not kept track of between centering moves,
   * and centering the cursor resets the state.
   */
  def centerOn(x:Int,y:Int) = { 
    movement = NoMovement
    _focusX = x
    _focusY = y

    setBounds()
  }

  /*
   * Move the cursor one cell space in a horizontal
   * of vertical direction. The cursor will keep track
   * of what cells became added by this move (covered by the cursor
   * or unmasked), and what cells became removed by this move
   * (no longer covered by the cursor or masked when previously unmasked).
   * The cursor will only keep the state of one move, so if two moves
   * are done in a row, the state of the first move is forgotten. Only
   * the difference between the cursor and it's most recent previous position
   * are accounted for.
   *
   * param     m     Movement enum that represents moving the cursor
   *                 Up,Down,Left or Right.
   */
  def move(m:Movement) = {
    movement = m
    m match {
      case Up => 
        addedRow = _ymin - 1
        removedRow = _focusY + dim
        _focusY -= 1
      case Down =>
        addedRow = _ymax + 1
        removedRow = _focusY - dim
        _focusY += 1
      case Left =>
        addedCol = _xmin - 1
        removedCol = _focusX + dim
        _focusX -= 1
      case Right =>
        addedCol = _xmax + 1
        removedCol = _focusX - dim
        _focusX += 1
      case _ => 
    }

    setBounds()
  }

  @inline final private def setBounds() = {
    _xmin = max(0,_focusX - dim)
    _xmax = min(r.cols - 1, _focusX + dim)
    _ymin = max(0, _focusY - dim)
    _ymax = min(r.rows - 1, _focusY + dim)
  }

  def setMask(f:(Int,Int) => Boolean) = {
    hasMask = true
    mask = new CursorMask(d,f)
  }

  /*
   * Iterates over all cell values of the raster which
   * are covered by the cursor and not masked.
   *
   * @param     f         Function that receives from each cell
   *                      it's x and y coordinates and it's value.
   */
  protected def foreach(cb: CursorCellCB):Unit = {
    if(!hasMask) {
      var y = _ymin
      var x = 0
      while(y <= _ymax) {
        x = _xmin
        while(x <= _xmax) {
          cb(x,y)
          x += 1
        }
        y += 1
      }
    } else {
      var y = 0
      while(y < d) {
        mask.foreachX(y) { x =>
          val xRaster = x + (_focusX-dim)
          val yRaster = y + (_focusY-dim)
          if(_xmin <= xRaster && xRaster <= _xmax && _ymin <= yRaster && yRaster <= _ymax) {
            cb(xRaster,yRaster)
          }
        }
        y += 1
      }
    }
  }

  /*
   * Iterates over all cell values of the raster which
   * are covered by the cursor and not masked, that were exposed
   * as part of the last move of the cursor.
   *
   * For instance, if move(Movement.Up) is called, then there will
   * potentially be a new row that is now covered by the cursor,
   * which are now covered. These values will be included for the
   * iterations of this function, as well any previously masked
   * cell values that were unmasked as part of the move.
   *
   * @param     f         Function that receives from each cell it's
   *                      x and y coordinates and it's value.
   */
  protected def foreachAdded(cb: CursorCellCB):Unit = {
    if(movement == NoMovement) {
      foreach(cb) 
    } else if (movement.isVertical) {
      if(0 <= addedRow && addedRow < r.rows) {
        if(!hasMask) {
          var x = _xmin
          while(x <= _xmax) {
            cb(x,addedRow)
            x += 1
          }
        } else {
          mask.foreachX(addedRow-(_focusY-dim)) { x =>
            val xRaster = x+(_focusX-dim)
            if(0 <= xRaster && xRaster <= r.rows) {
              cb(xRaster,addedRow)
            }
          }
        }
      }        
    } else { // Horizontal
      if(0 <= addedCol && addedCol < r.cols) {
        if(!hasMask) {
          var y = _ymin
          while(y <= _ymax) {
            cb(addedCol,y)
            y += 1
          }
        } else {
          if(movement == Left) {
            mask.foreachWestColumn { y =>
              val yRaster = y+(_focusY-dim)
              if(0 <= yRaster && yRaster < r.cols) {
                cb(addedCol,yRaster)
              }
            }
          } else { // Right
            mask.foreachEastColumn { y =>
              val yRaster = y+(_focusY-dim)
              if(0 <= yRaster && yRaster < r.cols) {
                cb(addedCol,yRaster)
              }
            }
          }
        }
      }        
    }

    if(hasMask) {
      mask.foreachUnmasked(movement) { (x,y) =>
        val xRaster = x+(_focusX-dim)
        val yRaster = y+(_focusY-dim)
        if(0 <= xRaster && xRaster < r.cols && 0 <= yRaster && yRaster < r.rows) {
          cb(xRaster,yRaster)
        }
      }
    }
  }

  /*
   * Iterates over all cell values of the raster which
   * are no longer covered by the cursor that were not previously masked
   * not masked, or that were masked when previously unmasked,
   * as part of the last move last move of the cursor.
   *
   * For instance, if move(Movement.Up) is called, then there will
   * potentially be a new row at the bottom of the cursor that is now
   * uncovered by the cursor. These values will be included for the
   * iterations of this function, as well any previously unmasked
   * cell values that were masked as part of the move.
   *
   * @param     f         Function that receives from each cell it's
   *                      x and y coordinates and it's value.
   */
  protected def foreachRemoved(cb: CursorCellCB):Unit = {
    if(movement == NoMovement) { return }

    if(movement.isVertical) {
      if(0 <= removedRow && removedRow < r.cols) {
        if(!hasMask) {
          var x = _xmin
          while(x <= _xmax) {
            cb(x,removedRow)
            x += 1
          }
        } else {
          if(movement == Up) {
            mask.foreachX(d-1) { x =>
              val xRaster = x+(_focusX-dim)
              if(0 <= xRaster && xRaster < r.cols) {
                cb(xRaster,removedRow)
              }
            }
          }
          else { // Down
            mask.foreachX(0) { x =>
              val xRaster = x+(_focusX-dim)
              if(0 <= xRaster && xRaster < r.cols) {
                cb(xRaster,removedRow)
              }
            }
          }
        }
      }
    } else { // Horizontal
      if(0 <= removedCol && removedCol < r.rows) {
        if(!hasMask) {
          var y = _ymin
          while(y <= _ymax) {
            cb(removedCol,y)
            y += 1
          }
        } else {
          if(movement == Left) {
            mask.foreachEastColumn { y =>
              val yRaster = y+(_focusY-dim)
              if(0 <= yRaster && yRaster < r.cols) {
                cb(removedCol,yRaster)
              }
            }
          } else { //Right
            mask.foreachWestColumn { y =>
              val yRaster = y+(_focusY-dim)
              if(0 <= yRaster && yRaster < r.cols) {
                cb(removedCol,yRaster)
              }
            }
          }
        }
      }
    }

    if(hasMask) {
      mask.foreachMasked(movement) { (x,y) =>
        val xRaster = x+(_focusX-dim)
        val yRaster = y+(_focusY-dim)
        if(0 <= xRaster && xRaster < r.cols && 0 <= yRaster && yRaster < r.rows) {
          cb(xRaster,yRaster)
        }
      }
    }
  }

  def asciiDraw:String = {
    var x = _xmin
    var y = _ymin
    var result = ""

    val mark = (x:Int, y:Int) => result += " " + getStr(x,y) + " "

    while(y <= _ymax) {
      x = _xmin
      while(x <= _xmax) {
        mark(x,y)
	x += 1
      }
      y += 1
      result += "\n"
    }
    result
  }

  def getStr(x:Int,y:Int):String
}

trait IntFocalValueCB { def act(v:Int):Unit }
trait IntFocalFoldCB { def act(a:Int,v:Int):Int }
trait IntFocalCellCB { def act(x:Int,y:Int,v:Int):Unit }
trait DoubleFocalValueCB { def act(v:Double):Unit }
trait DoubleFocalCellCB { def act(x:Int,y:Int,v:Double):Unit }
trait DoubleFocalFoldCB { def act(a:Double,v:Double):Double }

trait CursorCellCB { def apply(x:Int,y:Int):Unit }

case class IntCursorValueCB(cb:IntFocalValueCB,get:(Int,Int)=>Int) extends CursorCellCB {
  def apply(x:Int,y:Int):Unit = {
    cb.act(get(x,y))
  }
}

case class IntCursorCellCB(cb:IntFocalCellCB,get:(Int,Int)=>Int) extends CursorCellCB {
  def apply(x:Int,y:Int):Unit = {
    cb.act(x,y,get(x,y))
  }
}

case class DoubleCursorValueCB(cb:DoubleFocalValueCB,get:(Int,Int)=>Double) extends CursorCellCB {
  def apply(x:Int,y:Int):Unit = {
    cb.act(get(x,y))
  }
}

case class DoubleCursorCellCB(cb:DoubleFocalCellCB,get:(Int,Int)=>Double) extends CursorCellCB {
  def apply(x:Int,y:Int):Unit = {
    cb.act(x,y,get(x,y))
  }
}

class IntCursor(r:Raster, dim:Int) extends BaseCursor(r,dim) {
  val allCells = new IntCellSet {
    def foreach(cb:IntFocalValueCB) = IntCursor.this.foreach(new IntCursorValueCB(cb,get))
    def foreach(cb:IntFocalCellCB) = IntCursor.this.foreach(new IntCursorCellCB(cb,get))
  }

  val addedCells = new IntCellSet {
    def foreach(cb:IntFocalValueCB) = IntCursor.this.foreachAdded(new IntCursorValueCB(cb,get))
    def foreach(cb:IntFocalCellCB) = IntCursor.this.foreachAdded(new IntCursorCellCB(cb,get))
  }

  val removedCells = new IntCellSet {
    def foreach(cb:IntFocalValueCB) = IntCursor.this.foreachRemoved(new IntCursorValueCB(cb,get))
    def foreach(cb:IntFocalCellCB) = IntCursor.this.foreachRemoved(new IntCursorCellCB(cb,get))
  }

  def focusValue:Int = get(focusX,focusY)
  def get(x:Int,y:Int) = { raster.get(x,y) }
  def getStr(x:Int,y:Int):String = { "%d".format(get(x,y)) }
}

class DoubleCursor(r:Raster, dim:Int) extends BaseCursor(r,dim) {
  val allCells = new DoubleCellSet {
    def foreach(cb:DoubleFocalValueCB) = DoubleCursor.this.foreach(new DoubleCursorValueCB(cb,get))
    def foreach(cb:DoubleFocalCellCB) = DoubleCursor.this.foreach(new DoubleCursorCellCB(cb,get))
  }

  val addedCells = new DoubleCellSet {
    def foreach(cb:DoubleFocalValueCB) = DoubleCursor.this.foreachAdded(new DoubleCursorValueCB(cb,get))
    def foreach(cb:DoubleFocalCellCB) = DoubleCursor.this.foreachAdded(new DoubleCursorCellCB(cb,get))
  }

  val removedCells = new DoubleCellSet { 
    def foreach(cb:DoubleFocalValueCB) = DoubleCursor.this.foreachRemoved(new DoubleCursorValueCB(cb,get))
    def foreach(cb:DoubleFocalCellCB) = DoubleCursor.this.foreachRemoved(new DoubleCursorCellCB(cb,get))
  }

  def focusValue:Double = get(focusX,focusY)
  def get(x:Int,y:Int) = { raster.getDouble(x,y) }
  def getStr(x:Int,y:Int):String = { "%f".format(get(x,y)) }
}

