package geotrellis.spark.io.slippy

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.util.Filesystem

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
import org.apache.hadoop.fs.Path

import java.io.File
import scala.collection.JavaConversions._


class HadoopSlippyTileWriter[T](uri: String, extension: String)(getBytes: (GridKey, T) => Array[Byte])(implicit sc: SparkContext) extends SlippyTileWriter[T] {
  def setupWrite(zoom: Int, rdd: RDD[(GridKey, T)]): RDD[(GridKey, T)] = {
    val lZoom = zoom
    val lUri = uri
    val lExtension = extension
    val scheme = new Path("/Users").getFileSystem(sc.hadoopConfiguration).getScheme
    val keyToPath = { key: GridKey => new File(lUri, s"$lZoom/${key.col}/${key.row}.${lExtension}").getPath }
    rdd.setupSaveToHadoop(scheme, keyToPath, getBytes)
  }
}
