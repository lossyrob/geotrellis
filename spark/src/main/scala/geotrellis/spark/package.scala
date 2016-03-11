/*
 * Copyright (c) 2014 DigitalGlobe.
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

package geotrellis

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.util._

import geotrellis.spark.tiling._
import geotrellis.spark.ingest._
import geotrellis.spark.crop._
import geotrellis.spark.filter._

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

import spire.syntax.cfor._

import monocle.{Lens, PLens}
import monocle.syntax._

import scala.reflect.ClassTag
import scalaz.Functor

package object spark
    extends buffer.Implicits
    with mask.Implicits
    with merge.Implicits
    with reproject.Implicits
    with tiling.Implicits
    with stitch.Implicits
    with mapalgebra.Implicits
    with mapalgebra.local.Implicits
    with mapalgebra.local.time.Implicits
    with mapalgebra.focal.Implicits
    with mapalgebra.zonal.Implicits
    with summary.polygonal.Implicits
    with summary.Implicits
    with mapalgebra.focal.hillshade.Implicits
    with partitioner.Implicits
    with crop.Implicits
    with filter.Implicits
    with Serializable // required for java serialization, even though it's mixed in
{
  type TileLayerRDD[K] = RDD[(K, Tile)] with Metadata[LayerMetadata[K]]

  object TileLayerRDD {
    def apply[K](rdd: RDD[(K, Tile)], metadata: LayerMetadata[K]): TileLayerRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  type MultibandTileLayerRDD[K] = RDD[(K, MultibandTile)] with Metadata[LayerMetadata[K]]
  object MultibandTileLayerRDD {
    def apply[K](rdd: RDD[(K, MultibandTile)], metadata: LayerMetadata[K]): MultibandTileLayerRDD[K] =
      new ContextRDD(rdd, metadata)
  }

  type Component[T, C] = PLens[T, T, C, C]

  object Component {
    def apply[T, C](get: T => C, set: (T, C) => T): Component[T, C] =
      PLens[T, T, C, C](get)(c => t => set(t, c))
  }

  implicit def identityComponent[T]: Component[T, T] =
    Component(v => v, (_, v) => v)

  /** Describes a getter and setter for an object that has
    * an implicitly defined lens into a component of that object
    * with a specific type.
    */
  implicit class ComponentMethods[T](val self: T) extends MethodExtensions[T] {
    def getComponent[C]()(implicit component: Component[T, C]): C =
      component.get(self)

    def setComponent[C](value: C)(implicit component: Component[T, C]): T =
      component.set(value)(self)
  }

  type GridComponent[K] = Component[K, GridKey]
  type TimeComponent[K] = Component[K, TimeKey]

  type TileBounds = GridBounds

  /** Auto wrap a partitioner when something is requestion an Option[Partitioner];
    * useful for Options that take an Option[Partitioner]
    */
  implicit def partitionerToOption(partitioner: Partitioner): Option[Partitioner] =
    Some(partitioner)

  implicit class WithContextWrapper[K, V, M](val rdd: RDD[(K, V)] with Metadata[M]) {
    def withContext[K2, V2](f: RDD[(K, V)] => RDD[(K2, V2)]) =
      new ContextRDD(f(rdd), rdd.metadata)

    def mapContext[M2](f: M => M2) =
      new ContextRDD(rdd, f(rdd.metadata))
  }

  implicit def tupleToRDDWithMetadata[K, V, M](tup: (RDD[(K, V)], M)): RDD[(K, V)] with Metadata[M] =
    ContextRDD(tup._1, tup._2)

  implicit class withContextRDDMethods[K: ClassTag, V: ClassTag, M](rdd: RDD[(K, V)] with Metadata[M])
    extends ContextRDDMethods[K, V, M](rdd)

  implicit class withTileLayerRDDMethods[K](val self: TileLayerRDD[K])(implicit val keyClassTag: ClassTag[K])
    extends TileLayerRDDMethods[K]

  implicit class withTileLayerRDDMaskMethods[K: GridComponent: ClassTag](val self: TileLayerRDD[K])
      extends mask.TileLayerRDDMaskMethods[K]

  implicit class withMultibandTileLayerRDDMethods[K](val self: MultibandTileLayerRDD[K])(implicit val keyClassTag: ClassTag[K])
    extends MultibandTileLayerRDDMethods[K]

  implicit class withProjectedExtentRDDMethods[K: Component[?, ProjectedExtent], V <: CellGrid](val rdd: RDD[(K, V)]) {
    def toRasters: RDD[(K, Raster[V])] =
      rdd.mapPartitions({ partition =>
        partition.map { case (key, value) =>
          (key, Raster(value, key.getComponent[ProjectedExtent].extent))
        }
      }, preservesPartitioning = true)
  }

  implicit class withProjectedExtentTimeTilerKeyMethods[K: Component[?, ProjectedExtent]: Component[?, TimeKey]](val self: K) extends TilerKeyMethods[K, GridTimeKey] {
    def extent = self.getComponent[ProjectedExtent].extent
    def translate(spatialKey: GridKey): GridTimeKey = GridTimeKey(spatialKey, self.getComponent[TimeKey])
  }

  implicit class withProjectedExtentTilerKeyMethods[K: Component[?, ProjectedExtent]](val self: K) extends TilerKeyMethods[K, GridKey] {
    def extent = self.getComponent[ProjectedExtent].extent
    def translate(spatialKey: GridKey) = spatialKey
  }

  implicit class withCollectMetadataMethods[K1, V <: CellGrid](rdd: RDD[(K1, V)]) extends Serializable {
    def collectMetadata[K2: Boundable: GridComponent](crs: CRS, layoutScheme: LayoutScheme)
        (implicit ev: K1 => TilerKeyMethods[K1, K2]): (Int, LayerMetadata[K2]) = {
      LayerMetadata.fromRdd(rdd, crs, layoutScheme)
    }

    def collectMetadata[K2: Boundable: GridComponent](crs: CRS, layout: LayoutDefinition)
        (implicit ev: K1 => TilerKeyMethods[K1, K2]): LayerMetadata[K2] = {
      LayerMetadata.fromRdd(rdd, crs, layout)
    }
  }
}
