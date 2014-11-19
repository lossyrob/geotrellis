package climate.cmd

import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.ingest.{Ingest, Pyramid, HadoopIngestArgs}
import geotrellis.spark.io.hadoop.formats._

import org.apache.spark._
import com.quantifind.sumac.ArgMain

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object HDFSIngest extends ArgMain[HadoopIngestArgs] with Logging {
  def main(args: HadoopIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    val conf = args.hadoopConf
    conf.set("io.map.index.interval", "1")

    implicit val sparkContext = args.sparkContext("Ingest")

    val catalog: HadoopCatalog = HadoopCatalog(sparkContext, args.catalogPath)
    val source = sparkContext.netCdfRDD(args.inPath)

    val layoutScheme = ZoomedLayoutScheme(256)
    val (level, rdd) =  Ingest[NetCdfBand, SpaceTimeKey](source, args.destCrs, layoutScheme, true)

    val save = { (rdd: RasterRDD[SpaceTimeKey], level: LayoutLevel) =>
      catalog.save(LayerId(args.layerName, level.zoom), rdd, true)
    }

    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save) // expose exceptions
    } else{
      save(rdd, level).get
    }
  }
}