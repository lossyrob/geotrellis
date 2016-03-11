package geotrellis.spark.tiling

import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.util.MethodExtensions

import org.apache.spark.rdd._

import scala.reflect.ClassTag

class TilerMethods[K, V <: CellGrid: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V])](val self: RDD[(K, V)]) extends MethodExtensions[RDD[(K, V)]] {
  import Tiler.Options

  def cutTiles[K2: GridComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    CutTiles[K, K2, V](self, cellType, layoutDefinition, resampleMethod)

  def cutTiles[K2: GridComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    cutTiles(cellType, layoutDefinition, NearestNeighbor)

  def cutTiles[K2: GridComponent: ClassTag](rasterMetadata: LayerMetadata[K2], resampleMethod: ResampleMethod)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    cutTiles(rasterMetadata.cellType, rasterMetadata.layout, resampleMethod)

  def cutTiles[K2: GridComponent: ClassTag](rasterMetadata: LayerMetadata[K2])
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    cutTiles(rasterMetadata, NearestNeighbor)

  def tileToLayout[K2: GridComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition, options: Options)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    CutTiles[K, K2, V](self, cellType, layoutDefinition, options.resampleMethod)
      .merge(options.partitioner)

  def tileToLayout[K2: GridComponent: ClassTag](cellType: CellType, layoutDefinition: LayoutDefinition)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    tileToLayout(cellType, layoutDefinition, Options.DEFAULT)

  def tileToLayout[K2: GridComponent: ClassTag](rasterMetadata: LayerMetadata[K2], options: Options)
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    tileToLayout(rasterMetadata.cellType, rasterMetadata.layout, options)

  def tileToLayout[K2: GridComponent: ClassTag](rasterMetadata: LayerMetadata[K2])
      (implicit ev: K => TilerKeyMethods[K, K2]): RDD[(K2, V)] =
    tileToLayout(rasterMetadata, Options.DEFAULT)

}
