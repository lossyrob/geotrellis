package geotrellis.spark.reproject

import geotrellis.spark._
import geotrellis.spark.mapalgebra._
import geotrellis.spark.ingest._
import geotrellis.spark.tiling._
import geotrellis.spark.buffer._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.crop._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.reproject._
import geotrellis.raster.resample._
import geotrellis.raster.stitch._
import geotrellis.vector._
import geotrellis.vector.reproject._

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object TileRDDReproject {
  import Reproject.Options

  /** Reproject a set of buffered
    * @tparam           K           Key type; requires spatial component.
    * @tparam           V           Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            bufferedTiles                An RDD of buffered tiles, created using the BufferTiles operation.
    * @param            metadata           The raster metadata for this keyed tile set.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       Either the layout scheme or layout definition to use when re-keying the reprojected layers.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    */
  def apply[
    K: GridComponent: Boundable: ClassTag,
    V <: CellGrid: ClassTag: Stitcher: (? => TileReprojectMethods[V]): (? => CropMethods[V]): (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V])
  ](
    bufferedTiles: RDD[(K, BufferedTile[V])],
    metadata: LayerMetadata[K],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    options: Options
  ): (Int, RDD[(K, V)] with Metadata[LayerMetadata[K]]) = {
    val crs: CRS = metadata.crs
    val layout = metadata.layout
    val mapTransform: MapKeyTransform = layout.mapTransform
    val tileLayout: TileLayout = layout.tileLayout

    val rasterReprojectOptions =
      options.rasterReprojectOptions.parentGridExtent match {
        case Some(_) =>
          // Assume caller knows what she/he is doing
          options.rasterReprojectOptions
        case None =>
          if(options.matchLayerExtent) {
            val parentGridExtent = ReprojectRasterExtent(layout.toGridExtent, crs, destCrs, options.rasterReprojectOptions)
            options.rasterReprojectOptions.copy(parentGridExtent = Some(parentGridExtent))
          } else {
            options.rasterReprojectOptions
          }
      }

    val reprojectedTiles =
      bufferedTiles
        .mapPartitions { partition =>
          val transform = Transform(crs, destCrs)
          val inverseTransform = Transform(destCrs, crs)

          partition.map { case (key, BufferedTile(tile, gridBounds)) =>
            val innerExtent = mapTransform(key)
            val innerRasterExtent = RasterExtent(innerExtent, gridBounds.width, gridBounds.height)
            val outerGridBounds =
              GridBounds(
                -gridBounds.colMin,
                -gridBounds.rowMin,
                tile.cols - gridBounds.colMin - 1,
                tile.rows - gridBounds.rowMin - 1
              )
            val outerExtent = innerRasterExtent.extentFor(outerGridBounds, clamp = false)

            val window =
              if(options.matchLayerExtent) {
                gridBounds
              } else {
                // Reproject extra cells that are half the buffer size, as to avoid
                // any missed cells between tiles.

                GridBounds(
                  gridBounds.colMin / 2,
                  gridBounds.rowMin / 2,
                  (tile.cols + gridBounds.colMax - 1) / 2,
                  (tile.rows + gridBounds.rowMax - 1) / 2
                )
              }

            val Raster(newTile, newExtent) =
              tile.reproject(outerExtent, window, transform, inverseTransform, rasterReprojectOptions)

            ((key, newExtent), newTile)
          }
        }

    val (zoom, newMetadata) =
      targetLayout match {
        case Left(layoutScheme) =>
          LayerMetadata.fromRdd(reprojectedTiles, destCrs, layoutScheme)
        case Right(layoutDefinition) =>
          0 -> LayerMetadata.fromRdd(reprojectedTiles, destCrs, layoutDefinition)
      }

    val tiled = reprojectedTiles
      .tileToLayout(newMetadata, Tiler.Options(resampleMethod = options.rasterReprojectOptions.method, partitioner = bufferedTiles.partitioner))
    (zoom, ContextRDD(tiled, newMetadata))
  }

  /** Reproject a keyed tile RDD.
    *
    * @tparam           K           Key type; requires spatial component.
    * @tparam           V           Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            rdd                The keyed tile RDD.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       The layout scheme to use when re-keying the reprojected layers.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    */
  def apply[
    K: GridComponent: Boundable: ClassTag,
    V <: CellGrid: ClassTag: Stitcher: (? => TileReprojectMethods[V]): (? => CropMethods[V]): (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V])
  ](
    rdd: RDD[(K, V)] with Metadata[LayerMetadata[K]],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    options: Options
  ): (Int, RDD[(K, V)] with Metadata[LayerMetadata[K]]) = {
    val crs = rdd.metadata.crs
    val mapTransform = rdd.metadata.layout.mapTransform
    val tileLayout = rdd.metadata.layout.tileLayout

    val rasterExtents: RDD[(K, (RasterExtent, RasterExtent))] =
      rdd
        .mapPartitions({ partition =>
          val transform = Transform(crs, destCrs)

          partition.map { case (key, _) =>
            val extent = mapTransform(key)
            val rasterExtent = RasterExtent(extent, tileLayout.tileCols, tileLayout.tileRows)
            (key, (rasterExtent, ReprojectRasterExtent(rasterExtent, transform)))
          }
        }, preservesPartitioning = true)

    val borderSizesPerKey =
      rasterExtents
        .mapValues { case (re1, re2) =>
          // Reproject the extent back into the original CRS,
          // to determine how many border pixels we need.
          // Pad by one extra pixel.
          val e = re2.extent.reproject(destCrs, crs)
          val gb = re1.gridBoundsFor(e, clamp = false)
          BufferSizes(
            left = 1 + (if(gb.colMin < 0) -gb.colMin else 0),
            right = 1 + (if(gb.colMax >= re1.cols) gb.colMax - (re1.cols - 1) else 0),
            top = 1 + (if(gb.rowMin < 0) -gb.rowMin else 0),
            bottom = 1 + (if(gb.rowMax >= re1.rows) gb.rowMax - (re1.rows - 1) else 0)
          )
        }

    val bufferedTiles =
      rdd.bufferTiles(borderSizesPerKey)

    apply(bufferedTiles, rdd.metadata, destCrs, targetLayout, options)
  }

  /** Reproject this keyed tile RDD, using a constant border size for the operation.
    * @tparam           K                  Key type; requires spatial component.
    * @tparam           V                  Tile type; requires the ability to stitch, crop, reproject, merge, and create.
    *
    * @param            rdd                The keyed tile RDD.
    * @param            destCrs            The CRS to reproject to.
    * @param            targetLayout       The layout scheme to use when re-keying the reprojected layers.
    * @param            bufferSize         Number of pixels to buffer the tile with. The tile will only be buffered by this amount on
    *                                      any side if there is an adjacent, abutting tile to contribute the border pixels.
    * @param            options            Reprojection options.
    *
    * @return           The new zoom level and the reprojected keyed tile RDD.
    *
    * @note             This is faster than computing the correct border size per key, so if you know that a specific border size will be sufficient
    *                   to be accurate, e.g. if the CRS's are not very different and so the rasters will not skew heavily, then this method can be used
    *                   for performance benefit.
    */
  def apply[
    K: GridComponent: Boundable: ClassTag,
    V <: CellGrid: ClassTag: Stitcher: (? => TileReprojectMethods[V]): (? => CropMethods[V]): (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V])
  ](
    rdd: RDD[(K, V)] with Metadata[LayerMetadata[K]],
    destCrs: CRS,
    targetLayout: Either[LayoutScheme, LayoutDefinition],
    bufferSize: Int,
    options: Options
  ): (Int, RDD[(K, V)] with Metadata[LayerMetadata[K]]) =
    if(bufferSize == 0) {
      val fakeBuffers: RDD[(K, BufferedTile[V])] = rdd.withContext(_.mapValues { tile: V => BufferedTile(tile, GridBounds(0, 0, tile.cols - 1, tile.rows - 1)) })
      apply(fakeBuffers, rdd.metadata, destCrs, targetLayout, options)
    } else
      apply(rdd.bufferTiles(bufferSize), rdd.metadata, destCrs, targetLayout, options)

}
