package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.feature._
import geotrellis.process._
import geotrellis.testutil._

import org.scalatest.FunSpec
import org.scalatest.matchers._

class SumSpec extends ZonalSummarySpec {
  describe("Sum") {
    it("computes Sum") {
      val rData = createRasterSource(Array.fill(40*40)(1),4,4,10,10)
      val zone = Extent(10,-10,30,10).asFeature()

      val sumOp = rData.zonalSum(zone)
      run(sumOp) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (40)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("fails on nonintersecting zone") {
      // Test non-intersecting polygons (issue #412)
      val rData = createRasterSource(Array.fill(40*40)(1),4,4,10,10)
      val zone = Extent(10,-10,30,10).asFeature()

      val nonintersecting = Extent(100,120,100,120).asFeature(())
      val sumOp2 = rData.zonalSum(nonintersecting)
      run(sumOp2) match {
        case Complete(result,success) =>
          assert(false)
        case Error(msg,failure) =>
          msg should equal ("empty.reduceLeft")
      }
    }

    it("computes sum on raster source and 5 edge polygon") {
      val sum = 
        containedCells
          .map { case (col,row) => tiledR.get(col,row) }
          .foldLeft(0) { (a,b) => if(isNoData(b)) a else a + b }

      run(tiledRS.zonalSum(poly)) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (sum)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("computes Double Sum") {
      val rData = createRasterSource(Array.fill(40*40)(1),4,4,10,10)
      val zone = Extent(10,-10,30,10).asFeature()

      val sumDOp = rData.zonalSumDouble(zone)
      run(sumDOp) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (40.0)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }

    it("computes Double sum on raster source and 5 edge polygon") {
      val sum = 
        containedCells
          .map { case (col,row) => tiledRDouble.getDouble(col,row) }
          .foldLeft(0.0) { (a,b) => if(isNoData(b)) a else a + b }

      run(tiledRSDouble.zonalSumDouble(poly)) match {
        case Complete(result,success) =>
          //           println(success)
          result should equal (sum)
        case Error(msg,failure) =>
          println(msg)
          println(failure)
          assert(false)
      }
    }
  }
}
