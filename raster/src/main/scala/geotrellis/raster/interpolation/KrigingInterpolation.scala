/*
 * Copyright (c) 2015 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.interpolation

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.interpolation._

import spire.syntax.cfor._

object Test {
  class KrigingSimple(points: Array[PointFeature[Double]], bandwidth: Double, sv: Semivariogram) extends KrigingVectorBase {
    private def createPredictor(numberOfPoints: Int): (Double, Double) => (Double, Double) = {
      val unitCol: RealMatrix = MatrixUtils.createColumnRealMatrix(Array.fill(numberOfPoints)(1))

      //Covariogram Matrix
      val covarianceMatrix: RealMatrix =
        unitCol
          .multiply(unitCol.transpose())
          .scalarMultiply(sv.sill)
          .subtract(varianceMatrixGen(sv, points))
          .add(MatrixUtils.createRealIdentityMatrix(numberOfCells).scalarMultiply(sv.nugget))

      val pointValue: RealMatrix = MatrixUtils.createColumnRealMatrix(points.map(x => x.data))

      { (x, y) =>
        val distnaceSortedSimple = getPointDistancesSorted(points, 3, bandwidth, pointPredict)
        val distanceID: Array[Int] = distanceSortedSimple.map(_._1)
        //Local Covariances
        val CC: RealMatrix = new EigenDecomposition(covarianceMatrix.getSubMatrix(distanceID, distanceID)).getSolver.getInverse
        val d: RealMatrix = MatrixUtils.createColumnRealMatrix(distanceSortedSimple.map(_._2))
        //Local Covariance Vector
        val covVec: RealMatrix = unitCol.getSubMatrix(distanceID, Array(0)).scalarMultiply(sv.sill).subtract(MatrixUtils.createRealMatrix(Array.tabulate(d.getRowDimension, 1){(i, _) => sv(d.getEntry(i,0))}))
        cfor(0)(_ < d.getRowDimension, _ + 1) { j: Int =>
          if (d.getEntry(j, 0) == 0)
            covVec.setEntry(j, 0, covVec.getEntry(j, 0) + sv.nugget)
        }
        val mu: Double = points.foldLeft(0.0)(_ + _.data) / n
        val kTemp: RealMatrix = covVec.transpose().multiply(CC)
        val kPredict = mu + kTemp.multiply(pointValue.getSubMatrix(distanceID, Array(0)).subtract(unitCol.getSubMatrix(distanceID, Array(0)).scalarMultiply(mu))).getEntry(0, 0)
        val kVar = math.sqrt(sv.sill - kTemp.multiply(covVec).getEntry(0, 0))
        (kPredict, kVar)
      }
    }

    def predict(tile: Tile, extent: Extent): Tile = {
      val numberOfCells = tile.cols * tile.rows
      val rasterExtent = RasterExtent(extent, tile.cols, tile.rows)
      val unitCol: RealMatrix = MatrixUtils.createColumnRealMatrix(Array.fill(numberOfCells)(1))

      val krigingPrediction: Tile = DoubleArrayTile.empty(tile.cols, tile.rows)
      val predictor = createPredictor(tile.cols * tile.rows)

      cfor(0)(_ < tile.cols, _ + 1) { col =>
        cfor(0)(_ < tile.rows, _ + 1) { row =>
          val (x, y) = rasterExtent.gridToMap(col, row)
          val (kPredict, _) = predictor(x, y)

          krigingPrediction.setDouble(col, row, kPredict)
        }
      }

      krigingPrediction
    }

    def predict(pointMatrix: Array[Point]): Array[(Double, Double)] = {
      val n: Int = points.length
      if (n == 0)
        throw new IllegalArgumentException("No points in the training dataset")

      val krigingPrediction: Array[(Double, Double)] = Array.ofDim[(Double, Double)](pointMatrix.length)
      val predictor = createPredictor(n)

      cfor(0)(_ < pointMatrix.length, _ + 1) { i: Int =>
        val pointPredict: Point = pointMatrix(i)
        krigingPrediction(i) = predictor(pointPredict.x, pointPredict.y)
      }

      krigingPrediction
    }

  }
}

object KrigingInterpolation {

  private def isValid(point: Point, re: RasterExtent): Boolean =
    point.x >= re.extent.xmin && point.x <= re.extent.xmax && point.y <= re.extent.ymax && point.y >= re.extent.ymin

  def apply(method: KrigingVectorBase, points: Seq[PointFeature[Double]], re: RasterExtent, maxdist: Double, binmax: Double, model: ModelType): Tile = {
    model match {
      case Linear(_,_) => throw new UnsupportedOperationException("Linear semivariogram does not accept maxDist and maxBin values")
      case _ =>
        val cols = re.cols
        val rows = re.rows
        val tile = ArrayTile.alloc(TypeDouble, cols, rows)
        if(points.isEmpty)
          throw new UnsupportedOperationException("The set of points for constructing the prediction is empty")
        else {
          val rasterData: Array[Point] = Array.tabulate(rows * cols){i => Point(i/cols, i%cols)}
          val prediction: Array[(Double, Double)] = method.predict(rasterData)
          cfor(0)(_ < rows, _ + 1) { row =>
            cfor(0)(_ < cols, _ + 1) { col =>
              tile.setDouble(col, row, prediction(row*cols + col)._1)
            }
          }
          tile
        }
    }
  }

  def apply(method: KrigingVectorBase, points: Array[PointFeature[Double]], re: RasterExtent, chunkSize: Double, model: ModelType): Tile = {
    model match {
      case Linear(radius, lag) =>
        val cols = re.cols
        val rows = re.rows
        val tile = ArrayTile.alloc(TypeDouble, cols, rows)
        if (points.isEmpty) {
          throw new IllegalArgumentException("The set of points for constructing the prediction is empty")
        } else {
          val funcInterp = method.createPredictor()
          cfor(0)(_ < rows, _ + 1) { row =>
            cfor(0)(_ < cols, _ + 1) { col =>
              val (x, y) = re.gridToMap(col, row)
              val (v, _) = funcInterp(Point(x, y))

              tile.setDouble(col, row, v)
            }
          }
          tile
        }
      case _ => throw new UnsupportedOperationException("Non linear semivariograms do not accept radii and lags")
    }
  }
}
