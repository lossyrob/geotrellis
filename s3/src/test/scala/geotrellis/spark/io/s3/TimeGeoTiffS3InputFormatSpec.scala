package geotrellis.spark.io.s3

import java.nio.file.{ Paths, Path, Files }

import com.amazonaws.auth.AWSCredentials
import geotrellis.proj4.LatLng
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.ingest._
import geotrellis.util.Filesystem

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task._
import org.joda.time.format._
import org.scalatest._

class MockTimeGeoTiffS3RecordReader(context: TaskAttemptContext) extends TimeGeoTiffS3RecordReader(context) {
  override def getS3Client(credentials: AWSCredentials): S3Client = new MockS3Client
}

class MockTimeGeoTiffS3InputFormat extends TimeGeoTiffS3InputFormat {
  override def getS3Client(credentials: AWSCredentials): S3Client = new MockS3Client
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
    new MockTimeGeoTiffS3RecordReader(context)
}

class TimeGeoTiffS3InputFormatSpec extends FunSpec with Matchers with TestEnvironment {
  val layoutScheme = ZoomedLayoutScheme(LatLng)

  describe("GridTimeKey GeoTiff S3 InputFormat") {
    it("should read the time from a file") {
      val path = new java.io.File(inputHomeLocalPath, "test-time-tag.tif").getPath

      val format = new TimeGeoTiffS3InputFormat
      val conf = new Configuration(false)

      TimeGeoTiffS3InputFormat.setTimeTag(conf, "TIFFTAG_DATETIME")
      TimeGeoTiffS3InputFormat.setTimeFormat(conf, "2015:03:25 18:01:04")

      val context = new TaskAttemptContextImpl(conf, new TaskAttemptID())
      val rr = format.createRecordReader(null, context)
      val (key, tile) = rr.read("key", Filesystem.slurp(path))
      DateTimeFormat.forPattern("2015:03:25 18:01:04").print(key.time) should be ("2015:03:25 18:01:04")
    }

    val mockClient = new MockS3Client
    val testGeoTiffPath = "spark/src/test/resources/nex-geotiff/tasmax_amon_BCSD_rcp26_r1i1p1_CONUS_CCSM4_200601-201012-200601120000_0_0.tif"
    val geoTiffBytes = Files.readAllBytes(Paths.get(testGeoTiffPath))
    mockClient.putObject(this.getClass.getSimpleName, "nex-geotiff/tasmax.tiff", geoTiffBytes)

    it("should read GeoTiffs with ISO_TIME tag from S3") {
      val url = s"s3n://${this.getClass.getSimpleName}/nex-geotiff/"
      val job = sc.newJob("time-geotiff-ingest")
      S3InputFormat.setUrl(job, url)
      S3InputFormat.setAnonymous(job)
      TimeGeoTiffS3InputFormat.setTimeTag(job, "ISO_TIME")
      TimeGeoTiffS3InputFormat.setTimeFormat(job, "yyyy-MM-dd'T'HH:mm:ss")

      val source = sc.newAPIHadoopRDD(job.getConfiguration,
        classOf[MockTimeGeoTiffS3InputFormat],
        classOf[TimeProjectedExtent],
        classOf[Tile])

      source.cache
      val sourceCount = source.count
      sourceCount should not be (0)
      info(s"Source RDD count: ${sourceCount}")

      Ingest[TimeProjectedExtent, GridTimeKey](source, LatLng, layoutScheme){ (rdd, level) =>
        val rddCount = rdd.count
        rddCount should not be (0)
        info(s"Tiled RDD count: ${rddCount}")
      }
    }
  }
}
