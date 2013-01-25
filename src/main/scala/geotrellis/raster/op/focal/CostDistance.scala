package geotrellis.raster.op.focal

import geotrellis._
import java.util.PriorityQueue

/**
  * Generate a Cost-Distance raster based on a set of starting points and a cost
  * raster
  *
  * @param costOp Cost Raster (Int)
  * @param pointsOp List of starting points as tuples
  */
final case class CostDistance(costOp: Op[Raster], pointsOp: Op[Seq[(Int,Int)]]) extends Op[Raster] {
  def _run(context:Context) = runAsync(List(costOp,pointsOp))

  val nextSteps:Steps = {
    case List(cost, points) => costDistance(
      cost.asInstanceOf[Raster], points.asInstanceOf[List[(Int,Int)]])
  }  

  def isValid(c: Int, r: Int, cost: Raster):Boolean =
    c >= 0 && r >= 0 && c < cost.cols && r < cost.rows

  final class Dir(val dc: Int, val dr: Int) {
    val diag = !(dc == 0 || dr == 0)

    lazy val cornerOffsets = (dc,dr) match {
      case (-1,-1) => Array((0,-1),(-1,0))
      case ( 1,-1) => Array((0,-1),( 1,0))
      case (-1, 1) => Array((0, 1),(-1,0))
      case ( 1, 1) => Array((0, 1),( 1,0))
      case _ => Array[(Int,Int)]()
    }

    def apply(c: Int, r: Int) = (c+dc,r+dr)

    lazy val unitDistance = if (diag) 1.41421356237 else 1.0
  }

  val dirs:Array[Dir] = Array(
    (-1,-1), ( 0,-1), ( 1,-1),
    (-1, 0),          ( 1, 0),
    (-1, 1), ( 0, 1), ( 1, 1)).map { case (c,r) => new Dir(c,r) }


  /**
    * Input: 
    * (c,r) => Source cell
    * (dc,dr) => Delta (direction)
    * cost => Cost raster
    * d => C-D output raster
    * 
    * Output:
    * List((c,r)) <- list of cells set
    */    
  def calcCostCell(c: Int, r: Int, dir: Dir, cost: Raster, d: DoubleArrayRasterData) = {
    val cr = dir(c,r)

    if (isValid(cr._1,cr._2,cost)) {
      val prev = d.getDouble(cr._1, cr._2)
      if (prev == 0.0) { // This is a source cell, don't override and shortcircuit early
        None
      } else {
        val source = d.getDouble(c,r)

        // Previous value could be NODATA
        val prevCost = if (prev == NODATA || prev.isNaN) java.lang.Double.MAX_VALUE else prev

        var curMinCost = Double.MaxValue

        // Simple solution
        val baseCostOpt = calcCost(c, r, dir, cost)
        if (baseCostOpt.isDefined) {
          curMinCost = source + baseCostOpt.get
        }
                
        // Alternative solutions (going around the corner)
        // Generally you can check diags directly:
        // +---+---+---+
        // | a | b | c |
        // +---+---+---+
        // | d | e | f |
        // +---+---+---+
        // | g | h | i |
        // +---+---+---+
        //
        // For instance, "eg" can be computed directly
        // but it turns out "eg" can be more expensive
        // than "edg" or "ehg" so we compute those right
        // now just in case
        val cornerOffsets = dir.cornerOffsets
        val l = cornerOffsets.length
        var z = 0
        while(z < l) {
          val p = cornerOffsets(z)
          val c1 = p._1 + c
          val r1 = p._2 + r
          val cost1 = calcCost(c, r, c1, r1, cost)
          if (cost1.isDefined) {
            val cost2 = calcCost(c1, r1, dir(c,r), cost)
            if (cost2.isDefined) {
              curMinCost = math.min(curMinCost, source + cost1.get + cost2.get)
            }
          }
          z += 1
        }

        if (curMinCost == Double.MaxValue) {
          None // Possible all nodata values
        } else {
          if (curMinCost < prevCost) {
            d.setDouble(cr._1, cr._2, curMinCost)

            Some((cr._1, cr._2, curMinCost))
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  type Cost = (Int,Int,Double)

  def calcNeighbors(c: Int, r: Int, cost: Raster, d: DoubleArrayRasterData, p: PriorityQueue[Cost]) {
    val l = dirs.length
    var z = 0

    while(z < l) {
      val opt = calcCostCell(c, r, dirs(z), cost, d)
      if (opt.isDefined) {
        p.add(opt.get)
      }
      z += 1
    }
  }

  def factor(c: Int, r: Int, c1: Int, r1: Int) = if (c == c1 || r == r1) 1.0 else 1.41421356237

  def safeGet(c: Int, r: Int, cost: Raster):IOption = IOption(cost.get(c,r))

  def calcCost(c: Int, r: Int, c2: Int, r2: Int, cost: Raster):DOption = {
    val cost1 = safeGet(c,r,cost)
    val cost2 = safeGet(c2,r2,cost)

    if (cost1.isDefined && cost2.isDefined) {
      DOption(factor(c,r,c2,r2) * (cost1.get + cost2.get) / 2.0)
    } else {
      DOption.None
    }
  }

  def calcCost(c: Int, r: Int, cr2: (Int, Int), cost: Raster):DOption =
    calcCost(c,r,cr2._1,cr2._2,cost)

  def calcCost(c: Int, r: Int, dir: Dir, cost: Raster):DOption = 
    calcCost(c,r,dir(c,r),cost)

  def costDistance(cost: Raster, points: List[(Int,Int)]) = {
    val rasterExtent = cost.rasterExtent
    val output = DoubleArrayRasterData.empty(rasterExtent.cols, rasterExtent.rows)

    for((c,r) <- points)
      output.setDouble(c,r,0.0)

    val pqueue = new PriorityQueue(
        1000, new java.util.Comparator[Cost] {
          override def equals(a: Any) = a.equals(this)
          def compare(a: Cost, b: Cost) = a._3.compareTo(b._3)
        })

    for((c,r) <- points) {
      calcNeighbors(c,r, cost, output, pqueue)
    }

    var head:Cost = pqueue.poll
    while(head != null) {
      val c = head._1
      val r = head._2
      val v = head._3

      if (v == output.getDouble(c,r)) {
        calcNeighbors(c,r, cost, output, pqueue)
      }

      head = pqueue.poll
    }

    Result(Raster(output,rasterExtent))
  }
}

/**
  * Represents an optional integer
  * using 'Raster NODATA' as a flag
  */
private [focal] 
class IOption(val v: Int) extends AnyVal {
  def map(f: Int => Int) = if (isDefined) new IOption(f(v)) else this
  def flatMap(f: Int => IOption) = if (isDefined) f(v) else this
  def isDefined = v != NODATA
  def get = if (isDefined) v else sys.error("Get called on NODATA")
}

/**
  * Represents an optional integer
  * using 'Double.NaN' as a flag
  */
private [focal] 
class DOption(val v: Double) extends AnyVal {
  def map(f: Double => Double) = if (isDefined) new DOption(f(v)) else this
  def flatMap(f: Double => DOption) = if (isDefined) f(v) else this
  def isDefined = !v.isNaN
  def get = if (isDefined) v else sys.error("Get called on NaN")
}

private [focal] 
object IOption {
  val None = new IOption(NODATA)
  def apply(v: Int) = new IOption(v)
}

private [focal] 
object DOption {
  val None = new DOption(Double.NaN)
  def apply(v: Double) = new DOption(v)
}
