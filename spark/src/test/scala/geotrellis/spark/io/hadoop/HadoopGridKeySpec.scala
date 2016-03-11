package geotrellis.spark.io.hadoop

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles

class HadoopGridKeySpec
  extends PersistenceSpec[GridKey, Tile, LayerMetadata[GridKey]]
    with GridKeyIndexMethods
    with TestEnvironment
    with TestFiles
    with AllOnesTestTileTests {

  lazy val reader = HadoopLayerReader(outputLocal)
  lazy val writer = HadoopLayerWriter(outputLocal)
  lazy val deleter = HadoopLayerDeleter(outputLocal)
  lazy val copier = HadoopLayerCopier(outputLocal)
  lazy val mover  = HadoopLayerMover(outputLocal)
  lazy val reindexer = HadoopLayerReindexer(outputLocal)
  lazy val updater = HadoopLayerUpdater(outputLocal)
  lazy val tiles = HadoopTileReader[GridKey, Tile](outputLocal)
  lazy val sample = AllOnesTestFile
}
