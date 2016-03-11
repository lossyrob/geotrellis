package geotrellis.spark.etl.accumulo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.AccumuloLayerWriter
import geotrellis.spark.io.index.KeyIndexMethod

import org.apache.spark.SparkContext

class GridKeyAccumuloOutput extends AccumuloOutput[GridKey, Tile, TileLayerMetadata[GridKey]] {
  def writer(method: KeyIndexMethod[GridKey], props: Parameters)(implicit sc: SparkContext) =
    AccumuloLayerWriter(getInstance(props), props("table"), strategy(props)).writer[GridKey, Tile, TileLayerMetadata[GridKey]](method)
}
