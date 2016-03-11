package geotrellis.spark.io.hadoop

import geotrellis.raster._
import geotrellis.spark.TimeProjectedExtent
import geotrellis.vector._
import geotrellis.spark.io.hadoop.formats._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration

trait HadoopSparkContextMethods {
  val sc: SparkContext
  val defaultTiffExtensions: Seq[String] = Seq(".tif", ".TIF", ".tiff", ".TIFF")

  def hadoopGeoTiffRDD(path: String): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(new Path(path), defaultTiffExtensions)

  def hadoopGeoTiffRDD(path: String, tiffExtension: String): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(new Path(path), Seq(tiffExtension))

  def hadoopGeoTiffRDD(path: String, tiffExtensions: Seq[String] ): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(new Path(path), tiffExtensions)

  def hadoopGeoTiffRDD(path: Path): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(path, defaultTiffExtensions)

  def hadoopGeoTiffRDD(path: Path, tiffExtension: String): RDD[(ProjectedExtent, Tile)] =
    hadoopGeoTiffRDD(path, Seq(tiffExtension))

  def hadoopGeoTiffRDD(path: Path, tiffExtensions: Seq[String]): RDD[(ProjectedExtent, Tile)] =
    sc.newAPIHadoopRDD(
      sc.hadoopConfiguration.withInputDirectory(path, tiffExtensions),
      classOf[GeotiffInputFormat],
      classOf[ProjectedExtent],
      classOf[Tile]
    )

  def hadoopTimeGeoTiffRDD(path: String): RDD[(TimeProjectedExtent, Tile)] =
    hadoopTimeGeoTiffRDD(new Path(path), defaultTiffExtensions)

  def hadoopTimeGeoTiffRDD(path: String, tiffExtension: String): RDD[(TimeProjectedExtent, Tile)] =
    hadoopTimeGeoTiffRDD(new Path(path), Seq(tiffExtension))

  def hadoopTimeGeoTiffRDD(path: String, tiffExtensions: Seq[String] ): RDD[(TimeProjectedExtent, Tile)] =
    hadoopTimeGeoTiffRDD(new Path(path), tiffExtensions)

  def hadoopTimeGeoTiffRDD(path: Path): RDD[(TimeProjectedExtent, Tile)] =
    hadoopTimeGeoTiffRDD(path, defaultTiffExtensions)

  def hadoopTimeGeoTiffRDD(path: Path, tiffExtension: String): RDD[(TimeProjectedExtent, Tile)] =
    hadoopTimeGeoTiffRDD(path, Seq(tiffExtension))

  def hadoopTimeGeoTiffRDD(path: Path, tiffExtensions: Seq[String]): RDD[(TimeProjectedExtent, Tile)] =
    sc.newAPIHadoopRDD(
      sc.hadoopConfiguration.withInputDirectory(path, tiffExtensions),
      classOf[TimeGeoTiffInputFormat],
      classOf[TimeProjectedExtent],
      classOf[Tile]
    )

  def hadoopMultibandGeoTiffRDD(path: String): RDD[(ProjectedExtent, MultibandTile)] =
    hadoopMultibandGeoTiffRDD(new Path(path), defaultTiffExtensions)

  def hadoopMultibandGeoTiffRDD(path: String, tiffExtension: String): RDD[(ProjectedExtent, MultibandTile)] =
    hadoopMultibandGeoTiffRDD(new Path(path), Seq(tiffExtension))

  def hadoopMultibandGeoTiffRDD(path: String, tiffExtensions: Seq[String]): RDD[(ProjectedExtent, MultibandTile)] =
    hadoopMultibandGeoTiffRDD(new Path(path), tiffExtensions)

  def hadoopMultibandGeoTiffRDD(path: Path, tiffExtensions: Seq[String] = defaultTiffExtensions): RDD[(ProjectedExtent, MultibandTile)] =
    sc.newAPIHadoopRDD(
      sc.hadoopConfiguration.withInputDirectory(path, tiffExtensions),
      classOf[MultibandGeoTiffInputFormat],
      classOf[ProjectedExtent],
      classOf[MultibandTile]
    )

  def hadoopTimeMultibandGeoTiffRDD(path: String): RDD[(TimeProjectedExtent, MultibandTile)] =
    hadoopTimeMultibandGeoTiffRDD(new Path(path), defaultTiffExtensions)

  def hadoopTimeMultibandGeoTiffRDD(path: String, tiffExtension: String): RDD[(TimeProjectedExtent, MultibandTile)] =
    hadoopTimeMultibandGeoTiffRDD(new Path(path), Seq(tiffExtension))

  def hadoopTimeMultibandGeoTiffRDD(path: String, tiffExtensions: Seq[String]): RDD[(TimeProjectedExtent, MultibandTile)] =
    hadoopTimeMultibandGeoTiffRDD(new Path(path), tiffExtensions)

  def hadoopTimeMultibandGeoTiffRDD(path: Path, tiffExtensions: Seq[String] = defaultTiffExtensions): RDD[(TimeProjectedExtent, MultibandTile)] =
    sc.newAPIHadoopRDD(
      sc.hadoopConfiguration.withInputDirectory(path, tiffExtensions),
      classOf[TimeMultibandGeoTiffInputFormat],
      classOf[TimeProjectedExtent],
      classOf[MultibandTile]
    )

  def newJob: Job =
    Job.getInstance(sc.hadoopConfiguration)

  def newJob(name: String) =
    Job.getInstance(sc.hadoopConfiguration, name)
}
