/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.pointcloud.tiling

import io.pdal._

import geotrellis.spark._
import geotrellis.spark.tiling._

import org.apache.spark.rdd._
import spire.syntax.cfor._

import scala.collection.mutable
import scala.reflect.ClassTag

object CutPointCloud {
  def apply[
    K1: (? => TilerKeyMethods[K1, K2]),
    K2: SpatialComponent: ClassTag
  ](rdd: RDD[(K1, PointCloud)], layoutDefinition: LayoutDefinition): RDD[(K2, PointCloud)] = {
    val mapTransform = layoutDefinition.mapTransform
    val (tileCols, tileRows) = layoutDefinition.tileLayout.tileDimensions
    val tilePoints = tileCols * tileRows

    rdd
      .flatMap { case (inKey, pointCloud) =>
        val extent = inKey.extent

        mapTransform(extent)
          .coords
          .map  { spatialComponent =>
            val outKey    = inKey.translate(spatialComponent)
            val outExtent = mapTransform(outKey)
            val newBytes  = mutable.ArrayBuffer[Array[Byte]]()

            cfor(0)(_ < pointCloud.length, _ + 1) { i =>
              if(outExtent.contains(pointCloud.getX(i), pointCloud.getY(i)) && newBytes.length < tilePoints)
                newBytes += pointCloud.get(i)
            }

            val newPointCloud = PointCloud(
              bytes    = newBytes.flatten.toArray,
              dimTypes = pointCloud.dimTypes,
              metadata = pointCloud.metadata,
              schema   = pointCloud.schema
            )

            (outKey, newPointCloud)
          }
      }
  }
}