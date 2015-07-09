package geotrellis.spark.io.s3

import geotrellis.raster.Tile
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.index._

import org.apache.spark._
import spray.json.JsonFormat
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.retry.PredefinedRetryPolicies

import scala.reflect._

object S3RasterCatalog {
  def defaultS3Client = 
    () => {
      val provider = new DefaultAWSCredentialsProviderChain()
      val config = new com.amazonaws.ClientConfiguration
      config.setMaxConnections(128)
      config.setMaxErrorRetry(16)
      config.setConnectionTimeout(100000)
      config.setSocketTimeout(100000)
      config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
      new AmazonS3Client(provider, config)
    }
  
  private def layerPath(layerId: LayerId) = 
    s"${layerId.name}/${layerId.zoom}"  

  def apply(bucket: String)(implicit sc: SparkContext): S3RasterCatalog =
    apply(bucket, "", defaultS3Client)

  def apply(bucket: String, rootPath: String, s3client: () => S3Client = defaultS3Client)
    (implicit sc: SparkContext): S3RasterCatalog = {
    
    val attributeStore = new S3AttributeStore(s3client(), bucket, rootPath)
    new S3RasterCatalog(bucket, rootPath, attributeStore, s3client)
  }
}

class S3RasterCatalog(
  bucket: String,
  rootPath: String,
  val attributeStore: S3AttributeStore,    
  s3client: ()=>S3Client)
(implicit sc: SparkContext) extends AttributeCaching[S3LayerMetaData] {
  import S3RasterCatalog._

  def read[K: RasterRDDReader: Boundable: JsonFormat: ClassTag](layerId: LayerId, rasterQuery: RasterRDDQuery[K], numPartitions: Int = sc.defaultParallelism): RasterRDD[K] = {
    try {
      val metadata  = getLayerMetadata(layerId)
      val keyBounds = getLayerKeyBounds(layerId)                
      val index     = getLayerKeyIndex(layerId)

      val queryBounds = rasterQuery(metadata.rasterMetaData, keyBounds)
      implicitly[RasterRDDReader[K]].read(s3client, metadata, keyBounds, index, numPartitions)(layerId, queryBounds)
    } catch {
      case e: AttributeNotFoundError => throw new LayerNotFoundError(layerId)
    }
  }

  def read[K: RasterRDDReader: Boundable: JsonFormat: ClassTag](layerId: LayerId, numPartitions: Int): RasterRDD[K] =
    query[K](layerId, numPartitions).toRDD

  def read[K: RasterRDDReader: Boundable: JsonFormat: ClassTag](layerId: LayerId): RasterRDD[K] =
    query[K](layerId, sc.defaultParallelism).toRDD

  def query[K: RasterRDDReader: Boundable: JsonFormat: ClassTag](layerId: LayerId): BoundRasterRDDQuery[K] ={
    new BoundRasterRDDQuery[K](new RasterRDDQuery[K], read(layerId, _, sc.defaultParallelism))
  }

  def query[K: RasterRDDReader: Boundable: JsonFormat: ClassTag](layerId: LayerId, numPartitions: Int): BoundRasterRDDQuery[K] = {
    new BoundRasterRDDQuery[K](new RasterRDDQuery[K], read(layerId, _, numPartitions))
  }

  def writer[K: SpatialComponent: RasterRDDWriter: Boundable: JsonFormat: ClassTag](keyIndexMethod: KeyIndexMethod[K]): Writer[LayerId, RasterRDD[K]] =
    writer[K](keyIndexMethod, clobber = true)

  def writer[K: SpatialComponent: RasterRDDWriter: Boundable: JsonFormat: ClassTag](keyIndexMethod: KeyIndexMethod[K], subDir: String): Writer[LayerId, RasterRDD[K]] =
    writer[K](keyIndexMethod, subDir, clobber = true)

  def writer[K: SpatialComponent: RasterRDDWriter: Boundable: JsonFormat: ClassTag](keyIndexMethod: KeyIndexMethod[K], clobber: Boolean): Writer[LayerId, RasterRDD[K]] =
    writer[K](keyIndexMethod, "", clobber = true)  
  
  def writer[K: SpatialComponent: RasterRDDWriter: Boundable: JsonFormat: ClassTag](keyIndexMethod: KeyIndexMethod[K], subDir: String, clobber: Boolean): Writer[LayerId, RasterRDD[K]] =
    new Writer[LayerId, RasterRDD[K]] {
      def write(layerId: LayerId, rdd: RasterRDD[K]): Unit = {
        rdd.persist()

        val path = List(rootPath, subDir, layerPath(layerId)).filter(_.nonEmpty).mkString("/")

        val md = S3LayerMetaData(
            layerId = layerId,
            keyClass = classTag[K].toString,
            rasterMetaData = rdd.metaData,
            bucket = bucket,
            key = path)

        val keyBounds = implicitly[Boundable[K]].getKeyBounds(rdd)
        val index = {
          // Expanding spatial bounds? To allow multi-stage save?
          val indexKeyBounds = {
            val imin = keyBounds.minKey.updateSpatialComponent(SpatialKey(0, 0))
            val imax = keyBounds.maxKey.updateSpatialComponent(SpatialKey(rdd.metaData.tileLayout.layoutCols - 1, rdd.metaData.tileLayout.layoutRows - 1))
            KeyBounds(imin, imax)
          }
          keyIndexMethod.createIndex(indexKeyBounds)
        }

        setLayerMetadata(layerId, md)
        setLayerKeyBounds(layerId, keyBounds)
        setLayerKeyIndex(layerId, index)

        val rddWriter = implicitly[RasterRDDWriter[K]]
        rddWriter.write(s3client, bucket, path, keyBounds, index, clobber)(layerId, rdd)

        rdd.unpersist(blocking = false)
      }
    }

  def tileReader[K: TileReader: JsonFormat: ClassTag](layerId: LayerId): K => Tile = {
    val metadata  = getLayerMetadata(layerId)
    val keyBounds = getLayerKeyBounds(layerId)                
    val index     = getLayerKeyIndex(layerId)
    implicitly[TileReader[K]].read(s3client(), layerId, metadata, index, keyBounds)(_)    
  }
}
