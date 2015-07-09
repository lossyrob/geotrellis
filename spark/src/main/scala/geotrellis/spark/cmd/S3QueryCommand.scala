package geotrellis.spark.cmd

import geotrellis.spark._
import com.quantifind.sumac.FieldArgs
import geotrellis.spark.io.s3._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster.{Tile, GridBounds}
import geotrellis.spark.io.json._
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation.Required

import scala.reflect.ClassTag

class S3QueryArgs extends FieldArgs {
  @Required var bucket: String = _  
  @Required var key: String = _ 
  @Required var layer: String = _
  @Required var bbox: String = _

  def layerId: LayerId = {
    val c = layer.split(":")
    LayerId(c(0), c(1).toInt)
  }

  def bounds: GridBounds = {
    val c = layer.split(",")
    GridBounds(c(0).toInt, c(1).toInt, c(2).toInt, c(3).toInt)
  }
}

object S3QueryCommand extends ArgMain[S3QueryArgs] with Logging {
  def main(args: S3QueryArgs): Unit = {
    implicit val sc = SparkUtils.createSparkContext("S3 Query")

    val catalog = S3RasterCatalog(args.bucket, args.key)
    val attrib = catalog.attributeStore
    val lmd = attrib.read[S3LayerMetaData](args.layerId, "metaData")
    val md = lmd.rasterMetaData
    val bounds = md.gridBounds
    println("Catalog bounds: $bounds")    
    val rdd = catalog.query[SpatialKey](args.layerId).where(Intersects(args.bounds)).toRDD
    println(s"Expected Count: ${args.bounds.coords.length}")
    println(s"Record Count: ${rdd.count}")
  }
}
