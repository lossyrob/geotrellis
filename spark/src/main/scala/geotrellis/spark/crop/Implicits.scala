package geotrellis.spark.crop

import geotrellis.raster._
import geotrellis.spark._


object Implicits extends Implicits

trait Implicits {
  implicit class withTileLayerRDDCropMethods[K: GridComponent](val self: TileLayerRDD[K])
      extends TileLayerRDDCropMethods[K]
}
