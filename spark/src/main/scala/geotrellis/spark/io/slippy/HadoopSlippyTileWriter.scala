package geotrellis.spark.io.slippy

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.Filesystem
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._

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

class HadoopSlippyTileWriter[T](uri: Path, extension: String)(getBytes: (SpatialKey, T) => Array[Byte])(implicit sc: SparkContext) extends SlippyTileWriter[T] {
  def setupWrite(zoom: Int, rdd: RDD[(SpatialKey, T)]): RDD[(SpatialKey, T)] = {
    val lZoom = zoom
    val lUri = uri.toString
    val lExtension = extension
    val scheme = new Path("/Users").getFileSystem(sc.hadoopConfiguration).getScheme
    val keyToPath = { key: SpatialKey => new Path(lUri, s"$lZoom/${key.col}/${key.row}.${lExtension}").toString }
    rdd.setupSaveToHadoop(scheme, keyToPath, getBytes)
  }
}
