package geotrellis.vector.interpolation

import org.apache.commons.math3.analysis.{MultivariateMatrixFunction, MultivariateVectorFunction}
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresOptimizer.Optimum
import spire.syntax.cfor._

abstract class LeastSquaresFittingProblem(x: Array[Double], y: Array[Double], start: Array[Double]) {
  def valueFunc(w: Double, s: Double, a: Double): Double => Double
  def jacobianFunc(variables: Array[Double]): Double => Array[Double]

  def retMVF(): MultivariateVectorFunction = {
    new MultivariateVectorFunction {
      def value(variables: Array[Double]): Array[Double] = {
        val values: Array[Double] = Array.ofDim[Double](x.length)
        cfor(0)(_ < values.length, _ + 1) { i =>
          values(i) = valueFunc(variables(0), variables(1), variables(2))(x(i))
        }
        values
      }
    }
  }
  def retMMF(): MultivariateMatrixFunction = {
    def jacobianConstruct(variables: Array[Double]): Array[Array[Double]] = {
      val jacobianRet: Array[Array[Double]] = Array.ofDim[Double](x.length, 3)
      cfor(0)(_ < jacobianRet.length, _ + 1) { i =>
        jacobianRet(i) = jacobianFunc(variables)(x(i))
      }
      jacobianRet
    }
    new MultivariateMatrixFunction {
      override def value(doubles: Array[Double]): Array[Array[Double]] = jacobianConstruct(doubles)
    }
  }

  def optimum: Optimum = {
    val lsb: LeastSquaresBuilder = new LeastSquaresBuilder()
    val lmo: LevenbergMarquardtOptimizer = new LevenbergMarquardtOptimizer()

    lsb.model(retMVF(), retMMF())
    lsb.target(y)
    lsb.start(start)
    lsb.maxEvaluations(Int.MaxValue)
    lsb.maxIterations(Int.MaxValue)

    val lsp: LeastSquaresProblem = lsb.build
    lmo.optimize(lsp)
  }
}
