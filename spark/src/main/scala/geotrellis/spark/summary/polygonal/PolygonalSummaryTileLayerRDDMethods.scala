package geotrellis.spark.summary.polygonal

import geotrellis.raster.summary.polygonal._
import geotrellis.raster.histogram._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector._
import geotrellis.util.MethodExtensions
import org.apache.spark.Partitioner

import org.apache.spark.rdd._
import reflect.ClassTag


abstract class PolygonalSummaryTileLayerRDDMethods[K: ClassTag] extends MethodExtensions[TileLayerRDD[K]] {
  import Implicits._
  protected implicit val _sc: GridComponent[K]

  def polygonalSummary[T: ClassTag](
    polygon: Polygon,
    zeroValue: T,
    handler: TilePolygonalSummaryHandler[T]
  ): T =
    self
      .asRasters
      .map(_._2.asFeature)
      .polygonalSummary(polygon, zeroValue)(handler)

  def polygonalSummary[T: ClassTag](
    multiPolygon: MultiPolygon,
    zeroValue: T,
    handler: TilePolygonalSummaryHandler[T]
  ): T =
    self
      .asRasters
      .map(_._2.asFeature)
      .polygonalSummary(multiPolygon, zeroValue)(handler)

  def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                   polygon: Polygon,
                                                   zeroValue: T,
                                                   handler: TilePolygonalSummaryHandler[T],
                                                   fKey: K => L): RDD[(L, T)] = polygonalSummaryByKey(polygon, zeroValue, handler, fKey, None)

  def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                   polygon: Polygon,
                                                   zeroValue: T,
                                                   handler: TilePolygonalSummaryHandler[T],
                                                   fKey: K => L,
                                                   partitioner: Option[Partitioner]
  ): RDD[(L, T)] =
    self
      .asRasters
      .map { case (key, raster) => (fKey(key), raster.asFeature) }
      .polygonalSummaryByKey(polygon, zeroValue, partitioner)(handler)

  def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                   multiPolygon: MultiPolygon,
                                                   zeroValue: T,
                                                   handler: TilePolygonalSummaryHandler[T],
                                                   fKey: K => L): RDD[(L, T)] = polygonalSummaryByKey(multiPolygon, zeroValue, handler, fKey, None)

  def polygonalSummaryByKey[T: ClassTag, L: ClassTag](
                                                   multiPolygon: MultiPolygon,
                                                   zeroValue: T,
                                                   handler: TilePolygonalSummaryHandler[T],
                                                   fKey: K => L,
                                                   partitioner: Option[Partitioner]
  ): RDD[(L, T)] =
    self
      .asRasters
      .map { case (key, raster) => (fKey(key), raster.asFeature) }
      .polygonalSummaryByKey(multiPolygon, zeroValue, partitioner)(handler)

  def polygonalHistogram(polygon: Polygon): Histogram[Int] =
    polygonalSummary(polygon, FastMapHistogram(), HistogramSummary)

  def polygonalHistogram(multiPolygon: MultiPolygon): Histogram[Int] =
    polygonalSummary(multiPolygon, FastMapHistogram(), HistogramSummary)

  def polygonalMax(polygon: Polygon): Int =
    polygonalSummary(polygon, Int.MinValue, MaxSummary)

  def polygonalMax(multiPolygon: MultiPolygon): Int =
    polygonalSummary(multiPolygon, Int.MinValue, MaxSummary)

  def polygonalMaxDouble(polygon: Polygon): Double =
    polygonalSummary(polygon, Double.MinValue, MaxDoubleSummary)

  def polygonalMaxDouble(multiPolygon: MultiPolygon): Double =
    polygonalSummary(multiPolygon, Double.MinValue, MaxDoubleSummary)

  def polygonalMin(polygon: Polygon): Int =
    polygonalSummary(polygon, Int.MaxValue, MinSummary)

  def polygonalMin(multiPolygon: MultiPolygon): Int =
    polygonalSummary(multiPolygon, Int.MaxValue, MinSummary)

  def polygonalMinDouble(polygon: Polygon): Double =
    polygonalSummary(polygon, Double.MaxValue, MinDoubleSummary)

  def polygonalMinDouble(multiPolygon: MultiPolygon): Double =
    polygonalSummary(multiPolygon, Double.MaxValue, MinDoubleSummary)

  def polygonalMean(polygon: Polygon): Double =
    polygonalSummary(polygon, MeanResult(0.0, 0L), MeanSummary).mean

  def polygonalMean(multiPolygon: MultiPolygon): Double =
    polygonalSummary(multiPolygon, MeanResult(0.0, 0L), MeanSummary).mean

  def polygonalSum(polygon: Polygon): Long =
    polygonalSummary(polygon, 0L, SumSummary)

  def polygonalSum(multiPolygon: MultiPolygon): Long =
    polygonalSummary(multiPolygon, 0L, SumSummary)

  def polygonalSumDouble(polygon: Polygon): Double =
    polygonalSummary(polygon, 0.0, SumDoubleSummary)

  def polygonalSumDouble(multiPolygon: MultiPolygon): Double =
    polygonalSummary(multiPolygon, 0.0, SumDoubleSummary)

}
