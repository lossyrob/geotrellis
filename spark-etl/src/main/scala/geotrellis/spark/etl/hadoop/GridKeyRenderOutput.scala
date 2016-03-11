package geotrellis.spark.etl.hadoop

import java.math.BigInteger

import geotrellis.raster.Tile
import geotrellis.raster.render._
import geotrellis.spark.etl.OutputPlugin
import geotrellis.spark.io.index.KeyIndexMethod
import geotrellis.spark._
import geotrellis.spark.render._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.s3._

import org.apache.hadoop.conf.ConfServlet.BadFormatException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect._

class GridKeyRenderOutput extends OutputPlugin[GridKey, Tile, LayerMetadata[GridKey]] {
  def name = "render"
  def key = classTag[GridKey]
  def requiredKeys = Array("path", "encoding")
  def attributes(props: Map[String, String]) = null
  /**
   * Parses to a ColorClassifier a string of limits and their colors in hex RGBA
   * Only used for rendering PNGs
   *
   * @param classifications ex: "23:cc00ccff;30:aa00aaff;120->ff0000ff"
   * @return
   */
  def parseClassifications(classifications: Option[String]): Option[StrictColorClassifier[Int]] = {
    classifications.map { blob =>
      try {
        val split = blob.split(";").map(_.trim.split(":"))
        val limits = split.map(pair => Integer.parseInt(pair(0)))
        val colors = split.map(pair => new BigInteger(pair(1), 16).intValue())
        StrictColorClassifier(limits zip colors.map(RGBA(_)))
      } catch {
        case e: Exception =>
          throw new BadFormatException(s"Unable to parse classifications, expected '{limit}:{RGBA};{limit}:{RGBA};...' got: '$blob'")
      }
    }
  }

  override def apply(
    id: LayerId,
    rdd: RDD[(GridKey, Tile)] with Metadata[LayerMetadata[GridKey]],
    method: KeyIndexMethod[GridKey],
    props: Map[String, String]
  ): Unit = {
    val useS3 = (props("path").take(5) == "s3://")
    val images =
      props("encoding").toLowerCase match {
        case "png" =>
          rdd.asInstanceOf[RDD[(GridKey, Tile)] with Metadata[LayerMetadata[GridKey]]].renderPng(parseClassifications(props.get("breaks")))
        case "geotiff" =>
          rdd.asInstanceOf[RDD[(GridKey, Tile)] with Metadata[LayerMetadata[GridKey]]].renderGeoTiff()
      }

    if (useS3) {
      val keyToPath = SaveToS3Methods.spatialKeyToPath(id, props("path"))
      images.saveToS3(keyToPath)
    }
    else {
      val keyToPath = SaveToHadoopMethods.spatialKeyToPath(id, props("path"))
      images.saveToHadoop(keyToPath)
    }
  }

  def writer(method: KeyIndexMethod[GridKey], props: Parameters)(implicit sc: SparkContext) = ???
}
