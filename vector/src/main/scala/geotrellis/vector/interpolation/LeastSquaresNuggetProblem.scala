/*
  def optimum = {
        lsb.model(problem.retMVF(), problem.retMMF())
        lsb.target(problem.calculateTarget())
        lsb.start(problem.start)
        lsb.maxEvaluations(Int.MaxValue)
        lsb.maxIterations(Int.MaxValue)

        val lsp: LeastSquaresProblem = lsb.build
        lmo.optimize(lsp)
 }
 */

  // Similar treatment to LeastSquaresFittingProblem
  trait LeastSquaresFittingNuggetProblem {
    var x: Array[Double] = Array()
    var y: Array[Double] = Array()
    var start: Array[Double] = Array()

    def addPoint(Px: Double, Py: Double) = {
      x = x :+ Px
      y = y :+ Py
    }
    def calculateTarget(): Array[Double] = y

    def valueFuncNugget(w: Double, s: Double): Double => Double
    def jacobianFuncNugget(variables: Array[Double]): Double => Array[Double]

    def retMVF(): MultivariateVectorFunction = {
      new MultivariateVectorFunction {
        def value(variables: Array[Double]): Array[Double] = {
          val values: Array[Double] = Array.ofDim[Double](x.length)
          cfor(0)(_ < values.length, _ + 1) { i =>
            values(i) = valueFuncNugget(variables(0), variables(1))(x(i))
          }
          values
        }
      }
    }
    def retMMF(): MultivariateMatrixFunction = {
      def jacobianConstruct(variables: Array[Double]): Array[Array[Double]] = {
        val jacobianRet: Array[Array[Double]] = Array.ofDim[Double](x.length, 2)
        cfor(0)(_ < jacobianRet.length, _ + 1) { i =>
          jacobianRet(i) = jacobianFuncNugget(variables)(x(i))
        }
        jacobianRet
      }
      new MultivariateMatrixFunction {
        override def value(doubles: Array[Double]): Array[Array[Double]] = jacobianConstruct(doubles)
      }
    }
  }
