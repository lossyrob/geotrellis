package geotrellis.spark.etl.hadoop

import geotrellis.raster.MultibandTile
import geotrellis.spark.ingest._
import geotrellis.spark.io.hadoop._
import geotrellis.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TimeMultibandGeoTiffHadoopInput extends HadoopInput[TimeProjectedExtent, MultibandTile] {
  val format = "time-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(TimeProjectedExtent, MultibandTile)] = sc.hadoopTimeMultibandGeoTiffRDD(props("path"))
}

