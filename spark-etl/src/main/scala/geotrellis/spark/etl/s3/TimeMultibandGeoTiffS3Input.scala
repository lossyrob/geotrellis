package geotrellis.spark.etl.s3

import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark._
import geotrellis.spark.io.s3._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class TimeMultibandGeoTiffS3Input extends S3Input[TimeProjectedExtent, MultibandTile] {
  val format = "time-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(TimeProjectedExtent, MultibandTile)] =
    sc.newAPIHadoopRDD(configuration(props), classOf[TimeMultibandGeoTiffS3InputFormat], classOf[TimeProjectedExtent], classOf[MultibandTile])
}
