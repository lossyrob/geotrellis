package climate.cmd

import climate.op.PredicateCount
import com.quantifind.sumac.ArgMain
import geotrellis.raster._

import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import org.apache.hadoop.fs.Path
import org.apache.spark._
import climate.utils.Utils

class CalcArgs extends SparkArgs with AccumuloArgs

/**
 * Ingests raw multi-band NetCDF tiles into a re-projected and tiled RasterRDD
 */
object Calculate extends ArgMain[CalcArgs] with Logging {
  def main(args: CalcArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = args.sparkContext("Ingest")
    Utils.addClimateJar(sparkContext)

    //val accumulo = AccumuloInstance(args.instance, args.zookeeper, args.user, new PasswordToken(args.password))
    //val catalog = accumulo.catalog
    
    val catalog = HadoopCatalog(sparkContext, new Path("hdfs://localhost/catalog"))

    val rdd = catalog.load[SpaceTimeKey](LayerId("rcp45",1)).get
    println("PARTITIONS", rdd.partitions)

    val pred = { temp: Double => if (temp == Double.NaN) Double.NaN else if (temp > 0) 1 else 0 }
    val bin =  { key: SpaceTimeKey => key.updateTemporalComponent(key.temporalKey.time.withDayOfMonth(1).withMonthOfYear(1).withHourOfDay(0))}
    val ret = PredicateCount(TypeByte, pred, bin)(rdd);
    catalog.save[SpaceTimeKey](LayerId("over-0-daily",1), ret).get
  }
}
