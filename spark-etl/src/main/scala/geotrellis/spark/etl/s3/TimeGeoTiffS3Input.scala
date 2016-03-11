package geotrellis.spark.etl.s3

import geotrellis.raster.Tile
import geotrellis.spark.ingest._
import geotrellis.spark.io.s3._
import geotrellis.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class TimeGeoTiffS3Input extends S3Input[TimeProjectedExtent, Tile] {
  val format = "time-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(TimeProjectedExtent, Tile)] =
    sc.newAPIHadoopRDD(configuration(props), classOf[TimeGeoTiffS3InputFormat], classOf[TimeProjectedExtent], classOf[Tile])
}
