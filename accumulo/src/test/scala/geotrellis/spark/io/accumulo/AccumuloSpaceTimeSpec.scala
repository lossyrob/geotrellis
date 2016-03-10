package geotrellis.spark.io.accumulo

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

class AccumuloSpaceTimeSpec
  extends PersistenceSpec[SpaceTimeKey, Tile, RasterMetadata[SpaceTimeKey]]
    with SpaceTimeKeyIndexMethods
    with TestEnvironment
    with AccumuloTestEnvironment
    with TestFiles
    with CoordinateSpaceTimeTests
    with LayerUpdateSpaceTimeTileTests {
  implicit lazy val instance = MockAccumuloInstance()

  lazy val reader    = AccumuloLayerReader(instance)
  lazy val writer = AccumuloLayerWriter(instance, "tiles", SocketWriteStrategy())
  lazy val deleter   = AccumuloLayerDeleter(instance)
  lazy val reindexer = AccumuloLayerReindexer(instance, SocketWriteStrategy())
  lazy val updater   = AccumuloLayerUpdater(instance, SocketWriteStrategy())
  lazy val tiles     = AccumuloTileReader[SpaceTimeKey, Tile](instance)
  lazy val sample    = CoordinateSpaceTime
  lazy val copier = AccumuloLayerCopier(instance, reader, writer)
  lazy val mover  = AccumuloLayerMover(copier, deleter)
}
