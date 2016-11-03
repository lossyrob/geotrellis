package geotrellis.spark.etl.hadoop

import geotrellis.proj4.CRS
import geotrellis.raster.Tile
import geotrellis.spark.ingest._
import geotrellis.spark.io.hadoop._
import geotrellis.spark._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.io.hadoop.formats.TemporalGeoTiffInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TemporalGeoTiffHadoopInput extends HadoopInput[TemporalProjectedExtent, Tile] {
  val format = "temporal-geotiff"
  def apply(conf: EtlConf)(implicit sc: SparkContext): RDD[(TemporalProjectedExtent, Tile)] = {
    val options =
      HadoopGeoTiffRDD.Options(
        crs = conf.input.getCrs,
        maxTileSize = conf.input.maxTileSize,
        numPartitions = conf.input.numPartitions,
        timeTag    = conf.output.keyIndexMethod.timeTag.getOrElse(TemporalGeoTiffInputFormat.GEOTIFF_TIME_TAG_DEFAULT),
        timeFormat = conf.output.keyIndexMethod.timeFormat.getOrElse(TemporalGeoTiffInputFormat.GEOTIFF_TIME_FORMAT_DEFAULT)
      )

    HadoopGeoTiffRDD.temporal(getPath(conf.input.backend).path, options)
  }
}
