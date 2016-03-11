package geotrellis.spark.etl.hadoop

import geotrellis.raster.Tile
import geotrellis.spark.ingest._
import geotrellis.spark.io.hadoop._
import geotrellis.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TimeGeoTiffHadoopInput extends HadoopInput[TimeProjectedExtent, Tile] {
  val format = "time-geotiff"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(TimeProjectedExtent, Tile)] = sc.hadoopTimeGeoTiffRDD(props("path"))
}
