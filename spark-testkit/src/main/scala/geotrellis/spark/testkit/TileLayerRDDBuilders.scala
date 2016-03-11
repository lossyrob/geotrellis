package geotrellis.spark.testkit

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.ingest._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.github.nscala_time.time.Imports._
import scala.collection.mutable

trait TileLayerRDDBuilders {

  lazy val defaultCRS = LatLng

  /** Cuts the raster according to the layoutCols and layoutRows given.
    * Returns the raster that was used to fit inside the tile layout, which might
    * be smaller than the input raster
    *
    * Metadata mapTransform is accurate.
    */
  def createTileLayerRDD(
    input: Raster[Tile],
    layoutCols: Int,
    layoutRows: Int
  )(implicit sc: SparkContext): (Raster[Tile], TileLayerRDD[GridKey]) =
    createTileLayerRDD(input, layoutCols, layoutRows, defaultCRS)

  /** Cuts the raster according to the layoutCols and layoutRows given.
    * Returns the raser that was used to fit inside the tile layout, which might
    * be smaller than the input raster.
    *
    * Metadata mapTransform is accurate.
    */
  def createTileLayerRDD(
    raster: Raster[Tile],
    layoutCols: Int,
    layoutRows: Int,
    crs: CRS
  )(implicit sc: SparkContext): (Raster[Tile], TileLayerRDD[GridKey]) = {
    val Raster(input, extent) = raster
    val (cols, rows) = (input.cols, input.rows)

    val tileLayout =
      if (layoutCols >= cols || layoutRows >= rows)
        sys.error(s"Invalid for tile of dimensions ${(cols, rows)}: ${(layoutCols, layoutRows)}")
      else
        TileLayout(layoutCols, layoutRows, cols / layoutCols, rows / layoutRows)

    val tile: Tile =
      if(tileLayout.totalCols.toInt != cols || tileLayout.totalRows.toInt != rows) {
        input.crop(tileLayout.totalCols.toInt, tileLayout.totalRows.toInt)
      } else
        input

    val e = raster.rasterExtent.extentFor(GridBounds(0, 0, tile.cols - 1, tile.rows - 1))

    val resultRaster = Raster(tile, e)
    (resultRaster, createTileLayerRDD(sc, resultRaster, tileLayout, crs))
  }

  /** Cuts the tile according to the layoutCols and layoutRows given.
    * Returns the tile that was used to fit inside the tile layout, which might
    * be smaller than the input tile
    */
  def createTileLayerRDD(
    input: Tile,
    layoutCols: Int,
    layoutRows: Int
  )(implicit sc: SparkContext): (Tile, TileLayerRDD[GridKey]) =
    createTileLayerRDD(input, layoutCols, layoutRows, defaultCRS)

  /** Cuts the tile according to the layoutCols and layoutRows given.
    * Returns the tile that was used to fit inside the tile layout, which might
    * be smaller than the input tile
    */
  def createTileLayerRDD(
    input: Tile,
    layoutCols: Int,
    layoutRows: Int,
    crs: CRS
  )(implicit sc: SparkContext): (Tile, TileLayerRDD[GridKey]) = {
    val (cols, rows) = (input.cols, input.rows)

    val tileLayout =
      if (layoutCols >= cols || layoutRows >= rows)
        sys.error(s"Invalid for tile of dimensions ${(cols, rows)}: ${(layoutCols, layoutRows)}")
      else
        TileLayout(layoutCols, layoutRows, cols / layoutCols, rows / layoutRows)

    val tile: Tile =
      if(tileLayout.totalCols.toInt != cols || tileLayout.totalRows.toInt != rows) {
        input.crop(tileLayout.totalCols.toInt, tileLayout.totalRows.toInt)
      } else
        input

    (tile, createTileLayerRDD(sc, tile, tileLayout, crs))
  }

  def createTileLayerRDD(
    tile: Tile,
    tileLayout: TileLayout
  )(implicit sc: SparkContext): TileLayerRDD[GridKey] =
    createTileLayerRDD(sc, tile, tileLayout)

  def createTileLayerRDD(
    sc: SparkContext,
    tile: Tile,
    tileLayout: TileLayout
  ): TileLayerRDD[GridKey] =
    createTileLayerRDD(sc, tile, tileLayout, defaultCRS)

  def createTileLayerRDD(
    sc: SparkContext,
    tile: Tile,
    tileLayout: TileLayout,
    crs: CRS
  ): TileLayerRDD[GridKey] = {
    val extent = crs.worldExtent
    createTileLayerRDD(sc, Raster(tile, extent), tileLayout, crs)
  }

  def createTileLayerRDD(
    sc: SparkContext,
    raster: Raster[Tile],
    tileLayout: TileLayout
  ): TileLayerRDD[GridKey] =
    createTileLayerRDD(sc, raster, tileLayout, defaultCRS)

  def createTileLayerRDD(
    sc: SparkContext,
    raster: Raster[Tile],
    tileLayout: TileLayout,
    crs: CRS
  ): TileLayerRDD[GridKey] = {
    val layoutScheme = FloatingLayoutScheme(tileLayout.tileCols, tileLayout.tileRows)
    val inputRdd = sc.parallelize(Seq((ProjectedExtent(raster.extent, crs), raster.tile)))

    val (_, metadata) =
      TileLayerMetadata.fromRdd(inputRdd, crs, layoutScheme)

    val tiled: RDD[(GridKey, Tile)] = inputRdd.cutTiles(metadata)

    new ContextRDD(tiled, metadata)
  }

  def createGridTimeKeyTileLayerRDD(
    tiles: Traversable[(Tile, DateTime)],
    tileLayout: TileLayout,
    cellType: CellType = IntConstantNoDataCellType)(implicit sc: SparkContext): TileLayerRDD[GridTimeKey] = {

    val extent = defaultCRS.worldExtent
    val layout = LayoutDefinition(extent, tileLayout)
    val keyBounds = {
      val GridBounds(colMin, rowMin, colMax, rowMax) = layout.mapTransform(extent)
      val minTime = tiles.minBy(_._2)._2
      val maxTime = tiles.maxBy(_._2)._2
      KeyBounds(GridTimeKey(colMin, rowMin, minTime), GridTimeKey(colMax, rowMax, maxTime))
    }
    val metadata = TileLayerMetadata(
      cellType,
      layout,
      extent,
      defaultCRS,
      keyBounds
    )

    val re = RasterExtent(
      extent = extent,
      cols = tileLayout.layoutCols,
      rows = tileLayout.layoutRows
    )

    val tileBounds = re.gridBoundsFor(extent)

    val tmsTiles = mutable.ListBuffer[(GridTimeKey, Tile)]()

    for( (tile, time) <- tiles) {
      val adjustedTile =
        if (tile.cols == tileLayout.totalCols.toInt &&
          tile.rows == tileLayout.totalRows.toInt) tile
        else CompositeTile.wrap(tile, tileLayout, cropped = false)

      tmsTiles ++=
        tileBounds.coords.map { case (col, row) =>

          val targetRasterExtent =
            RasterExtent(
              extent = re.extentFor(GridBounds(col, row, col, row)),
              cols = tileLayout.tileCols,
              rows = tileLayout.tileRows
            )

          val subTile: Tile = adjustedTile.resample(extent, targetRasterExtent)

          (GridTimeKey(col, row, time), subTile)
        }
    }

    new ContextRDD(sc.parallelize(tmsTiles), metadata)
  }
}
