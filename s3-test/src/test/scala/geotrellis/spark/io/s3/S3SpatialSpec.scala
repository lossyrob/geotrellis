package geotrellis.spark.io.s3

import geotrellis.raster.Tile
import geotrellis.spark.io._
import geotrellis.spark.io.s3.testkit._
import geotrellis.spark.io.index._
import geotrellis.spark.testfiles.TestFiles
import geotrellis.spark._

import org.scalatest._

class S3SpatialSpec
  extends PersistenceSpec[SpatialKey, Tile, TileLayerMetadata[SpatialKey]]
    with SpatialKeyIndexMethods
    with TestEnvironment with TestFiles
    with AllOnesTestTileSpec {

  lazy val bucket = "mock-bucket"
  lazy val prefix = "catalog"

  registerAfterAll { () =>
    MockS3Client.reset()
  }

  lazy val attributeStore = new S3AttributeStore(bucket, prefix) {
    override val s3Client = new MockS3Client()
  }

  lazy val rddReader =
    new S3RDDReader {
      def getS3Client = () => new MockS3Client()
    }

  lazy val rddWriter =
    new S3RDDWriter {
      def getS3Client = () => new MockS3Client()
    }

  lazy val reader = new MockS3LayerReader(attributeStore)
  lazy val creader = new MockS3CollectionLayerReader(attributeStore)
  lazy val writer = new MockS3LayerWriter(attributeStore, bucket, prefix)
  lazy val updater = new S3LayerUpdater(attributeStore, reader) { override def rddWriter = S3SpatialSpec.this.rddWriter }
  lazy val deleter = new S3LayerDeleter(attributeStore) { override val getS3Client = () => new MockS3Client() }
  lazy val copier  = new S3LayerCopier(attributeStore, bucket, prefix) { override val getS3Client = () => new MockS3Client }
  lazy val reindexer = GenericLayerReindexer[S3LayerHeader](attributeStore, reader, writer, deleter, copier)
  lazy val mover = GenericLayerMover(copier, deleter)
  lazy val tiles = new S3ValueReader(attributeStore) { override val s3Client = new MockS3Client()  }
  lazy val sample = AllOnesTestFile
}
