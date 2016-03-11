package geotrellis.spark.etl.hadoop

import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MultibandGeoTiffSequenceHadoopInput extends HadoopInput[ProjectedExtent, MultibandTile] {
  val format = "multiband-geotiff-sequence"
  def apply(props: Parameters)(implicit sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] =
    sc
      .sequenceFile[String, Array[Byte]](props("path"))
      .map { case (path, bytes) =>
        val geotiff = GeoTiffReader.readMultiband(bytes)
        (ProjectedExtent(geotiff.extent, geotiff.crs), geotiff.tile)
      }
}
