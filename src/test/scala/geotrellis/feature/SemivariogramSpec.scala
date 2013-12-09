package geotrellis.feature

import geotrellis._

import org.scalatest.FunSpec
import org.scalatest.matchers._

import org.scalatest.junit.JUnitRunner
import geotrellis.testutil._

import org.apache.commons.math3.stat.regression.SimpleRegression

class SemivariogramSpec extends FunSpec 
                            with ShouldMatchers 
                            with TestServer 
                            with RasterBuilders {
  describe("Semivariogram") {
    it("Semivariogram (Bucketed)") {
      val points = Seq[Point[Int]](
        Point(0.0,0.0,10),
        Point(1.0,0.0,20),
        Point(4.0,4.0,60),
        Point(0.0,6.0,80)
        )

      /* would make pairs:
        1. (0,0,10) (1,0,20) // dist: 1.0
        2. (0,0,10) (4,4,60) // dist: 5.657
        3. (0,0,10) (0,6,80) // dist: 6
        4. (1,0,20) (4,4,60) // dist: 5
        5. (1,0,20) (0,6,80) // dist: 6.083
        6. (4,4,60) (0,6,80) // dist: 4.472
  
        Buckets:
          {0,2} // sv: 50   <-(20-10)^2 / 2
          {2,4} // sv: NaN 
          {4,6} // sv: 750  <-((60-10)^2 + (60-20)^2 + (80-60)^2) / 3 / 2
          {6,8} // sv: 2125 <-((80-10)^2 + (80-20)^2) / 2 / 2
        
        Regression on points:
          {1,50}, {5,750}, {7,2125}
      */

      val slope = 321.42857
      val intercept = -417.85714
      val sv = (x:Double) => slope*x + intercept


      val semivariogram = Semivariogram(points,None,2,Linear)
      semivariogram(0) should be (sv(0) plusOrMinus 0.0001)
      semivariogram(10) should be (sv(10) plusOrMinus 0.0001)
    }

    it("Semivariogram (Bucketed w/ Limit)") {
      val points = Seq[Point[Int]](
        Point(0.0,0.0,10),
        Point(1.0,0.0,20),
        Point(4.0,4.0,60),
        Point(0.0,6.0,80)
        )

      /* would make pairs:
        1. (0,0,10) (1,0,20) // dist: 1.0
        2. (0,0,10) (4,4,60) // dist: 5.657
        3. (0,0,10) (0,6,80) // dist: 6
        4. (1,0,20) (4,4,60) // dist: 5
        5. (4,4,60) (0,6,80) // dist: 4.472

        removed:
        X. (1,0,20) (0,6,80) // dist: 6.083
  
        Buckets:
          {0,2} // sv: 50   <-(20-10)^2 / 2
          {2,4} // sv: NaN 
          {4,6} // sv: 750  <-((60-10)^2 + (60-20)^2 + (80-60)^2) / 3 / 2
          {6,8} // sv: 2450 <-(80-10)^2 / 2
        
        Regression on points:
          {1,50}, {5,750}, {7,2450}
      */

      val slope = 367.85714
      val intercept = -510.71429
      val sv = (x:Double) => slope*x + intercept
      val limit:Option[Int] = Some(6)

      val semivariogram = Semivariogram(points,limit,2,Linear)
      semivariogram(0) should be (sv(0) plusOrMinus 0.0001)
      semivariogram(10) should be (sv(10) plusOrMinus 0.0001)
    }

    it("Semivariogram (Non-Bucketed)") {
      val points = Seq[Point[Int]](
        Point(0.0,0.0,10),
        Point(0.0,0.0,16),
        Point(1.0,0.0,20),
        Point(0.0,1.0,24),
        Point(2.0,2.0,50)
        )

      /* would make pairs:
        1. (0,0,10) (0,0,16) // dist: 0
        2. (0,0,10) (1,0,20) // dist: 1
        3. (0,0,10) (0,1,24) // dist: 1
        4. (0,0,10) (2,2,50) // dist: srt(8)
        5. (0,0,16) (1,0,20) // dist: 1
        6. (0,0,16) (0,1,24) // dist: 1
        7. (0,0,16) (2,2,50) // dist: srt(8)
        8. (1,0,20) (0,1,24) // dist: sqrt(2)
        9. (1,0,20) (2,2,50) // dist: srt(5)
        10.(0,1,24) (2,2,50) // dist: srt(5)

        dist: 0 // sv: 18 <-(16-10)^2 / 2
        dist: 1 // sv: 47 <-((20-10)^2 + (24-10)^2 + (20-16)^2 + (24-16)^2) / 4 / 2
        dist: sqrt(2) // sv: 8 <-((24-20)^2 / 2
        dist: sqrt(5) // sv: 394 <-((50-20)^2 + (50-24)^2) / 2 / 2
        dist: sqrt(8) // sv: 689 <-((50-10)^2 + (50-16)^2) / 2 / 2
        
        Regression on points:
          {0,18}
          {1,47}
          {sqrt(2),8}
          {sqrt(5),394}
          {sqrt(8),689}
      */
      
      val slope = 240.77389
      val intercept = -128.93555
      val sv = (x:Double) => slope*x + intercept

      val semivariogram = Semivariogram(points,None,0,Linear)
      semivariogram(0) should be (sv(0) plusOrMinus 0.0001)
      semivariogram(10) should be (sv(10) plusOrMinus 0.0001)
    }

    it("Semivariogram (Non-Bucketed w/ Limit)") {
      val points = Seq[Point[Int]](
        Point(0.0,0.0,10),
        Point(0.0,0.0,16),
        Point(1.0,0.0,20),
        Point(0.0,1.0,24),
        Point(2.0,2.0,50)
        )

      /* would make pairs:
        1. (0,0,10) (0,0,16) // dist: 0
        2. (0,0,10) (1,0,20) // dist: 1
        3. (0,0,10) (0,1,24) // dist: 1
        
        4. (0,0,16) (1,0,20) // dist: 1
        5. (0,0,16) (0,1,24) // dist: 1

        6. (1,0,20) (0,1,24) // dist: sqrt(2)
        7. (1,0,20) (2,2,50) // dist: srt(5)
        8. (0,1,24) (2,2,50) // dist: srt(5)

        removed:
        X. (0,0,10) (2,2,50) // dist: srt(8)
        X. (0,0,16) (2,2,50) // dist: srt(8)

        dist: 0 // sv: 18 <-(16-10)^2 / 2
        dist: 1 // sv: 47 <-((20-10)^2 + (24-10)^2 + (20-16)^2 + (24-16)^2) / 4 / 2
        dist: sqrt(2) // sv: 8 <-((24-20)^2 / 2
        
        Regression on points:
          {0,18}
          {1,47}
          {sqrt(2),8}
      */
      
      val slope = -0.40878
      val intercept = 24.66229
      val sv = (x:Double) => slope*x + intercept
      val limit:Option[Int] = Some(2)

      val semivariogram = Semivariogram(points,limit,0,Linear)
      semivariogram(0) should be (sv(0) plusOrMinus 0.0001)
      semivariogram(10) should be (sv(10) plusOrMinus 0.0001)
    }
  }
}
