package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.feature._
import geotrellis.process._
import geotrellis.testutil._

import org.scalatest.FunSpec
import org.scalatest.matchers._

class MaxSpec extends ZonalSummarySpec {
  describe("Max") {
    it("computes Maximum") {
      val rData = createRasterSource(Array.fill(40*40)(1),4,4,10,10)
      val zone = Extent(10,-10,30,10).asFeature()

      val maxOp = rData.zonalMax(zone)
      run(maxOp) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (1)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("computes max on raster source and 5 edge polygon") {
      val max = 
        containedCells
          .map { case (col,row) => tiledR.get(col,row) }
          .foldLeft(Int.MinValue) { (a,b) => if(isNoData(b)) a else math.max(a, b) }

      run(tiledRS.zonalMax(poly)) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (max)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("computes Double Maximum") {
      val rData = createRasterSource(Array.fill(40*40)(1),4,4,10,10)
      val zone = Extent(10,-10,30,10).asFeature()

      val maxDOp = rData.zonalMaxDouble(zone)
      run(maxDOp) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (1.0)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("computes max on double raster source and 5 edge polygon") {
      val max = 
        containedCells
          .map { case (col,row) => tiledRDouble.getDouble(col,row) }
          .foldLeft(Double.MinValue) { (a,b) => if(isNoData(b)) a else math.max(a, b) }

      run(tiledRSDouble.zonalMaxDouble(poly)) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (max)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }
  }
}
