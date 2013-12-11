package geotrellis.benchmark

import geotrellis._
import geotrellis.source._
import geotrellis.raster.op._
import geotrellis.raster.op.local._
import geotrellis.statistics.op.stat._
import geotrellis.io._
import geotrellis.render.op._

import com.google.caliper.Param

object WeightedAdd extends BenchmarkRunner(classOf[WeightedAdd])
class WeightedAdd extends OperationBenchmark {
  // val names = Array("SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k")
  // val weights = Array(2, 1, 5, 2)

  // val names = Array("SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k")
  // val weights = Array(2, 1, 5, 2)

  val names = Array("SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k",
                    "SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k",
                    "SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k",
                    "SBN_farm_mkt", "SBN_RR_stops_walk", "SBN_inc_percap", "SBN_street_den_1k")
  val weights = Array(2, 1, 5, 2,
                      2, 1, 5, 2,
                      2, 1, 5, 2,
                      2, 1, 5, 2)

  // val names = Array("SBN_farm_mkt", "SBN_RR_stops_walk")
  // val weights = Array(2, 3)

  @Param(Array("256","512", "1024", "2048", "4096"))
  var size:Int = 0

  var op:Op[Raster] = null
  var source:RasterSource = null
  var sourceSeq:RasterSource = null

  override def setUp() {
    val n = names.length
    val re = getRasterExtent(names(0), size, size)
    val total = weights.sum
    val rs = (0 until n).map(i => Multiply(LoadRaster(names(i), re), weights(i)))

    op = Add(rs: _*)

    source = 
      (0 until n).map(i => RasterSource(names(i),re) * weights(i))
                 .reduce(_+_)

    sourceSeq = 
      RasterSeqSource2((0 until n).map(i => RasterSource(names(i),re) * weights(i)))
                 .localAdd

  }

  // target
  def timeWeightedAddOp(reps:Int) = run(reps)(weightedAddOp)
  def weightedAddOp = get(op)

  def timeWeightedAddSource(reps:Int) = run(reps)(weightedAddSource)
  def weightedAddSource = get(source)

  def timeWeightedAddSourceSeq(reps:Int) = run(reps)(weightedAddSourceSeq)
  def weightedAddSourceSeq = get(sourceSeq)
}
