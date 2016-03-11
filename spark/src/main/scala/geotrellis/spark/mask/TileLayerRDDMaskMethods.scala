package geotrellis.spark.mask

import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterize.Options
import geotrellis.spark._
import geotrellis.spark.TileLayerRDD
import geotrellis.vector._
import geotrellis.util.MethodExtensions
import scala.reflect.ClassTag


abstract class TileLayerRDDMaskMethods[K: GridComponent: ClassTag] extends MethodExtensions[TileLayerRDD[K]] {

  // As done by [[geotrellis.raster.rasterize.polygon.TestLineSet]] in [[geotrellis.raster.rasterize.polygon.PolygonRasterizer]].
  private def eliminateNotQualified(geom: Option[Geometry]): Option[Geometry] = {

    def rec(geom: GeometryCollection): GeometryCollection = geom match {
      case GeometryCollection(_, lines, polygons, multiPoints, multiLines, multiPolygons, geometryCollections) =>
        GeometryCollection(
          Seq(),
          lines.filter(_.envelope.area != 0),
          polygons,
          multiPoints,
          multiLines,
          multiPolygons,
          geometryCollections.map(rec))
    }

    geom match {
      case Some(g: Line) if g.envelope.area == 0 => None
      case Some(_: Point) => None
      case Some(g: GeometryCollection) => Some(rec(g))
      case _ => geom
    }
  }

  private def _mask(masker: (Extent, Tile) => Tile) = {
    val mapTransform = self.metadata.mapTransform
    val rdd =
      self.map { case (k, tile) =>
        val key = k.getComponent[GridKey]
        val tileExtent = mapTransform(key)
        val result = masker(tileExtent, tile)
        (k, result)
      }
    ContextRDD(rdd, self.metadata)
  }

  /** Masks this raster by the given Polygon. */
  def mask(geom: Polygon): TileLayerRDD[K] = mask(Seq(geom), Options.DEFAULT)

  def mask(geom: Polygon, options: Options): TileLayerRDD[K] = mask(Seq(geom), options)

  /** Masks this raster by the given Polygons. */
  def mask(geoms: Traversable[Polygon]): TileLayerRDD[K] = mask(geoms, Options.DEFAULT)

  def mask(geoms: Traversable[Polygon], options: Options): TileLayerRDD[K] =
    _mask { case (tileExtent, tile) =>
      val tileGeoms = geoms.flatMap { g =>
        val intersections = g.safeIntersection(tileExtent).toGeometry()
        eliminateNotQualified(intersections)
      }
      tile.mask(tileExtent, tileGeoms, options)
    }

  /** Masks this raster by the given MultiPolygon. */
  def mask(geom: MultiPolygon): TileLayerRDD[K] = mask(geom, Options.DEFAULT)

  def mask(geom: MultiPolygon, options: Options): TileLayerRDD[K] = mask(Seq(geom), options)

  /** Masks this raster by the given MultiPolygons. */
  def mask(geoms: Traversable[MultiPolygon], options: Options)(implicit d: DummyImplicit): TileLayerRDD[K] =
    _mask { case (tileExtent, tile) =>
      val tileGeoms = geoms.flatMap { g =>
        val intersections = g.safeIntersection(tileExtent).toGeometry()
        eliminateNotQualified(intersections)
      }
      tile.mask(tileExtent, tileGeoms, options)
    }

  /** Masks this raster by the given Extent. */
  def mask(ext: Extent, options: Options = Options.DEFAULT): TileLayerRDD[K] =
    _mask { case (tileExtent, tile) =>
      val tileExts = ext.intersection(tileExtent)
      tileExts match {
        case Some(intersected) if intersected.area != 0 => tile.mask(tileExtent, intersected.toPolygon(), options)
        case _ => ArrayTile.empty(tile.cellType, tile.cols, tile.rows)
      }
    }

}
