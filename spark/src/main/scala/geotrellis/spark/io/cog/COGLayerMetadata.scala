package geotrellis.spark.io.cog

import geotrellis.proj4.CRS
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector.Extent
import geotrellis.vector.io._

import spray.json._
import spray.json.DefaultJsonProtocol._

case class COGLayerMetadata[K: SpatialComponent](
  cellType: CellType,
  zoomRangeInfos: Vector[(ZoomRange, KeyBounds[K])], // KeyBounds is for a minZoom in this ranges
  layoutScheme: ZoomedLayoutScheme,
  extent: Extent,
  crs: CRS
) {
  private val maxZooms =
    zoomRangeInfos.map(_._1.maxZoom).toArray

  def zoomRanges: Vector[ZoomRange] =
    zoomRangeInfos.map(_._1)

  def zoomRangeFor(zoom: Int): ZoomRange =
    zoomRangeInfoFor(zoom)._1

  def zoomRangeInfoFor(zoom: Int): (ZoomRange, KeyBounds[K]) = {
    val i = java.util.Arrays.binarySearch(maxZooms, zoom)
    val idx =
      if(i >= 0) { i }
      else {
        ~i //- 1
      }

    zoomRangeInfos(idx)
  }


  def layoutForZoom(z: Int): LayoutDefinition =
    layoutScheme.levelForZoom(z).layout

  def tileLayerMetadata(zoom: Int) =
    TileLayerMetadata[K](
      cellType,
      layoutScheme.levelForZoom(zoom).layout,
      extent,
      crs,
      zoomRangeInfoFor(zoom)._2
    )

  /** Returns the ZoomRange to read, and a Sequence of SpatialKey COGs to read, the total
    * GridBounds to read from that COG, and the sequence of GridBounds -> Keys that that
    * file should be cropped by
    */
  def getReadDefinitions(queryKeyBounds: KeyBounds[SpatialKey], zoom: Int): (ZoomRange, Seq[(SpatialKey, Int, GridBounds, Seq[(GridBounds, SpatialKey)])]) = {
    val zoomRange @ ZoomRange(minZoom, maxZoom) = zoomRangeFor(zoom)
    val (baseLayout, layout) = layoutForZoom(minZoom) -> layoutForZoom(zoom)

    val overviewIdx = maxZoom - zoom - 1
    val KeyBounds(queryMinKey, queryMaxKey) = queryKeyBounds

    // queryKeyBounds converted on a base zoom level
    val baseQueryKeyBounds = {
      val KeyBounds(minKey, maxKey) = queryKeyBounds

      val baseMinKey =
        baseLayout
          .mapTransform
          .pointToKey(
            layout
              .mapTransform
              .keyToExtent(minKey)
              .center
          )

      val baseMaxKey =
        baseLayout
          .mapTransform
          .pointToKey(
            layout
              .mapTransform
              .keyToExtent(maxKey)
              .center
          )

      KeyBounds(baseMinKey, baseMaxKey)
    }

    val GridBounds(colMin, rowMin, colMax, rowMax) = baseQueryKeyBounds.toGridBounds()

    val seq =
      for {
        col <- colMin to colMax
        row <- rowMin to rowMax
      } yield {
        val baseKey = SpatialKey(col, row)
        val layoutGridBounds = layout.mapTransform(baseKey.extent(baseLayout))

        val seq =
          (for {
            qcol <- queryMinKey.col to queryMaxKey.col
            qrow <- queryMinKey.row to queryMaxKey.row
          } yield {
            val key = SpatialKey(qcol, qrow)
            val keyLayoutGridBounds = layout.mapTransform(key.extent(layout))

            if(layoutGridBounds.contains(keyLayoutGridBounds)) {
              val (minCol, minRow) = ((qcol - layoutGridBounds.colMin) * baseLayout.tileCols, (qrow - layoutGridBounds.rowMin) * baseLayout.tileRows)
              val (maxCol, maxRow) = (minCol + layout.tileCols - 1, minRow + layout.tileRows - 1)
              Some((GridBounds(minCol, minRow, maxCol, maxRow), key))
            } else None
          }).flatten

        if(seq.nonEmpty) {
          val combinedGridBounds = seq.map(_._1).reduce(_ combine _)
          Some((baseKey, overviewIdx, combinedGridBounds, seq))
        } else None
      }

    (zoomRange, seq.flatten)
  }

  /** Returns the ZoomRange and SpatialKey of the COG to be read for this key, index of overview, as well as the GridBounds to crop
    * that COG to */
  def getReadDefinition(key: SpatialKey, zoom: Int): (ZoomRange, SpatialKey, Int, GridBounds) = {
    val zoomRange @ ZoomRange(minZoom, maxZoom) = zoomRangeFor(zoom)
    val overviewIdx = maxZoom - zoom - 1

    val baseLayout = layoutForZoom(minZoom)
    val layout = layoutForZoom(zoom)

    val baseKey =
      baseLayout
        .mapTransform
        .pointToKey(
          layout
            .mapTransform
            .keyToExtent(key)
            .center
        )

    val layoutGridBounds = layout.mapTransform(baseKey.extent(baseLayout))

    val gridBounds = {
      val gb = layoutGridBounds
      val (minCol, minRow) = ((key.col - gb.colMin) * layout.tileCols, (key.row - gb.rowMin) * layout.tileRows)
      val (maxCol, maxRow) = (minCol + layout.tileCols - 1, minRow + layout.tileRows - 1)
      GridBounds(minCol, minRow, maxCol, maxRow)
    }

    (zoomRange, baseKey, overviewIdx, gridBounds)
  }
}

object COGLayerMetadata {
  /** Constructs a COGLayerMetadata
    *
    * @param cellType: CellType of layer.
    * @param extent: The extent of the layer.
    * @param crs: CRS of layer.
    * @param keyBounds: KeyBounds of the base zoom level for the layer.
    * @param layoutScheme: The ZoomedLayoutScheme of this layer.
    * @param maxZoom: The maximum zoom level for this tile
    * @param minZoom: Minimum zoom level. Defaults to 0.
    * @param maxTileSize: The maximum tile size for any one COG file for this layer.
    *                     For instance, if 1024, no COG in the layer will have a greater
    *                     width or height than 1024. Defaults to 4096.
    */
  def apply[K: SpatialComponent](
    cellType: CellType,
    extent: Extent,
    crs: CRS,
    keyBounds: KeyBounds[K],
    layoutScheme: ZoomedLayoutScheme,
    maxZoom: Int,
    minZoom: Int = 0,
    maxTileSize: Int = 4096
  ): COGLayerMetadata[K] = {

    val baseLayout = layoutScheme.levelForZoom(maxZoom).layout

    val pmin =
      baseLayout.mapTransform.keyToExtent(keyBounds.minKey.getComponent[SpatialKey]).center

    val pmax =
      baseLayout.mapTransform.keyToExtent(keyBounds.maxKey.getComponent[SpatialKey]).center

    def getKeyBounds(layout: LayoutDefinition): KeyBounds[K] = {
      val (skMin, skMax) =
        (layout.mapTransform.pointToKey(pmin), layout.mapTransform.pointToKey(pmax))
      KeyBounds(
        keyBounds.minKey.setComponent[SpatialKey](skMin),
        keyBounds.maxKey.setComponent[SpatialKey](skMax)
      )
    }

    // List of ranges, the current maximum zoom for the next range, the current tile size
    // for the range, and a flag for whether or not we've gotten to a zoom level that
    // has 4 or less tiles contain the extent.
    val accSeed = (List[(ZoomRange, KeyBounds[K])](), maxZoom, baseLayout.tileRows, false)

    val (zoomRanges, _, _, _) =
      (maxZoom to minZoom by -1).foldLeft(accSeed) { case ((acc, currMaxZoom, currTileSize, isMinLevel), z) =>
        if(isMinLevel) {
          val thisLayout = layoutScheme.levelForZoom(z).layout

          ((ZoomRange(z, currMaxZoom), getKeyBounds(thisLayout)) :: acc, z - 1, currTileSize, isMinLevel)
        } else {
          val thisLayout = layoutScheme.levelForZoom(z).layout
          val thisTileSize =
            if(currMaxZoom == z) {
              // Starting a fresh range
              thisLayout.tileRows
            } else {
              currTileSize * 2
            }

          val thisIsMinLevel = {
            val SpatialKey(colMin, rowMin) = thisLayout.mapTransform.pointToKey(extent.xmin, extent.ymax)
            val SpatialKey(colMax, rowMax) = thisLayout.mapTransform.pointToKey(extent.xmax, extent.ymin)
            rowMax - rowMin < 2 || colMax - colMin < 2
          }

          if(thisIsMinLevel || thisTileSize >= maxTileSize || z == minZoom) {
            // thisTileSize is ignored next round
            ((ZoomRange(z, currMaxZoom), getKeyBounds(thisLayout)) :: acc, z - 1, thisTileSize, thisIsMinLevel)
          } else {
            (acc, currMaxZoom, thisTileSize, thisIsMinLevel)
          }
        }
      }

    COGLayerMetadata(
      cellType,
      zoomRanges.toVector,
      layoutScheme,
      extent,
      crs
    )
  }

  implicit def cogLayerMetadataFormat[K: SpatialComponent: JsonFormat] =
    new RootJsonFormat[COGLayerMetadata[K]] {
      def write(metadata: COGLayerMetadata[K]) =
        JsObject(
          "cellType" -> metadata.cellType.toJson,
          "zoomRangesInfos" -> metadata.zoomRangeInfos.toJson,
          "layoutScheme" -> metadata.layoutScheme.toJson,
          "extent" -> metadata.extent.toJson,
          "crs" -> metadata.crs.toJson
        )

      def read(value: JsValue): COGLayerMetadata[K] =
        value.asJsObject.getFields("cellType", "zoomRangesInfos", "layoutScheme", "extent", "crs") match {
          case Seq(cellType, JsArray(zoomRanges), layoutScheme, extent, crs) =>
            COGLayerMetadata(
              cellType.convertTo[CellType],
              zoomRanges.map(_.convertTo[(ZoomRange, KeyBounds[K])]),
              layoutScheme.convertTo[ZoomedLayoutScheme],
              extent.convertTo[Extent],
              crs.convertTo[CRS]
            )
          case v =>
            throw new DeserializationException(s"COGLayerMetadata expected, got $v")
        }
    }
}
