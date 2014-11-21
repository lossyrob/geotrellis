package geotrellis.spark.ingest

import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.tiling._
import geotrellis.vector.Extent
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken

import org.apache.hadoop.fs._

import org.apache.spark._

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required

import scala.reflect.ClassTag

class AccumuloIngestArgs extends IngestArgs with AccumuloArgs {
  @Required var table: String = _  
}

object AccumuloIngestCommand extends ArgMain[AccumuloIngestArgs] with Logging {
  def main(args: AccumuloIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    val conf = args.hadoopConf
    conf.set("io.map.index.interval", "1")

    implicit val sparkContext = args.sparkContext("Ingest")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val source = sparkContext.hadoopGeoTiffRDD(args.inPath)
    val layoutScheme = ZoomedLayoutScheme(256)
    val (level, rdd) =  Ingest[ProjectedExtent, SpatialKey](source, args.destCrs, layoutScheme)

    val save = { (rdd: RasterRDD[SpatialKey], level: LayoutLevel) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, args.clobber)
    }
    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save).get // expose exceptions
    } else{
      save(rdd, level).get
    }
  }
}

class AccumuloPyramidArgs extends AccumuloArgs with SparkArgs with HadoopArgs {
  @Required var layerName: String = _
  @Required var table: String = _  
  @Required var startLevel: Int = _
}


object AccumuloPyramidCommand extends ArgMain[AccumuloPyramidArgs] with Logging {
  def main(args: AccumuloPyramidArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    val conf = args.hadoopConf
    conf.set("io.map.index.interval", "1")

    implicit val sparkContext = args.sparkContext("Ingest")

    val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    val catalog = accumulo.catalog

    val rdd = catalog.load[SpatialKey](LayerId(args.layerName, args.startLevel)).get

    val layoutScheme = ZoomedLayoutScheme(256)
    val level = layoutScheme.levelFor(args.startLevel)

    val save = { (rdd: RasterRDD[SpatialKey], level: LayoutLevel) =>
      accumulo.catalog.save(LayerId(args.layerName, level.zoom), args.table, rdd, true)
    }

    Pyramid.saveLevels(rdd, level, layoutScheme)(save)
  }
}
