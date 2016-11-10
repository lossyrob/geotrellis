package geotrellis.spark.io.accumulo

import geotrellis.raster.{Tile, TileFeature}
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestTileFeatureFiles

class AccumuloTileFeatureSpaceTimeSpec
  extends PersistenceSpec[SpaceTimeKey, TileFeature[Tile, Tile], TileLayerMetadata[SpaceTimeKey]]
    with SpaceTimeKeyIndexMethods
    with TestEnvironment
    with AccumuloTestEnvironment
    with TestTileFeatureFiles
    with CoordinateSpaceTimeTileFeatureSpec
    with LayerUpdateSpaceTimeTileFeatureSpec {
  implicit lazy val instance = MockAccumuloInstance()

  lazy val reader    = AccumuloLayerReader(instance)
  lazy val creader   = AccumuloCollectionLayerReader(instance)
  lazy val writer    = AccumuloLayerWriter(instance, "tiles", SocketWriteStrategy())
  lazy val deleter   = AccumuloLayerDeleter(instance)
  lazy val reindexer = AccumuloLayerReindexer(instance, SocketWriteStrategy())
  lazy val updater   = AccumuloLayerUpdater(instance, SocketWriteStrategy())
  lazy val tiles     = AccumuloValueReader(instance)
  lazy val sample    = CoordinateSpaceTime
  lazy val copier    = AccumuloLayerCopier(instance, reader, writer)
  lazy val mover     = AccumuloLayerMover(copier, deleter)
}
