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

case class COGLayerMetadata[K](
  cellType: CellType,
  zoomRangeInfos: Vector[(ZoomRange, KeyBounds[K])],
  layoutScheme: ZoomedLayoutScheme,
  extent: Extent,
  crs: CRS
) {
  private val maxZooms =
    zoomRangeInfos.map(_._1.maxZoom).toArray

  def zoomRanges: Vector[ZoomRange] =
    zoomRangeInfos.map(_._1)

  def zoomRangeFor(zoom: Int): ZoomRange = {
    val i = java.util.Arrays.binarySearch(maxZooms, zoom)
    val idx =
      if(i >= 0) { i }
      else {
        ~i - 1
      }
    zoomRangeInfos(idx)._1
  }


  def layoutForZoom(z: Int): LayoutDefinition =
    layoutScheme.levelForZoom(z).layout

  /** Returns the ZoomRange to read, and a Sequence of SpatialKey COGs to read, the total
    * GridBounds to read from that COG, and the sequence of GridBounds -> Keys that that
    * file should be cropped by
    */
  def getReadDefinitions(keyBounds: KeyBounds[SpatialKey], zoom: Int): (ZoomRange, Seq[(SpatialKey, Int, GridBounds, Seq[(GridBounds, SpatialKey)])]) = {
    ???
  }

  /** Returns the ZoomRange and SpatialKey of the COG to be read for this key, index of overview, as well as the GridBounds to crop
    * that COG to */
  def getReadDefinition(key: SpatialKey, zoom: Int): (ZoomRange, SpatialKey, Int, GridBounds) = {
    val ZoomRange(minZoom, maxZoom) = zoomRangeFor(zoom)
    val overviewIdx = maxZoom - zoom

    ???
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
      (maxZoom to minZoom by -1).foldLeft(accSeed) { case ((acc, currMaxZoom, currTileSize, isLowLevel), z) =>
        if(isLowLevel) {
          val thisLayout = layoutScheme.levelForZoom(z).layout

          ((ZoomRange(z, currMaxZoom), getKeyBounds(thisLayout)) :: acc, z - 1, currTileSize, isLowLevel)
        } else {
          val thisLayout = layoutScheme.levelForZoom(z).layout
          val thisTileSize =
            if(currMaxZoom == z) {
              // Starting a fresh range
              thisLayout.tileRows
            } else {
              currTileSize * 2
            }

          val thisIsLowLevel = {
            val SpatialKey(colMin, rowMin) = thisLayout.mapTransform.pointToKey(extent.xmin, extent.ymax)
            val SpatialKey(colMax, rowMax) = thisLayout.mapTransform.pointToKey(extent.xmax, extent.ymin)
            rowMax - rowMin < 2 || colMax - colMin < 2
          }

          if(thisIsLowLevel || thisTileSize >= maxTileSize) {
            // thisTileSize is ignored next round
            ((ZoomRange(z, currMaxZoom), getKeyBounds(thisLayout)) :: acc, z - 1, thisTileSize, thisIsLowLevel)
          } else {
            (acc, currMaxZoom, thisTileSize, thisIsLowLevel)
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

  implicit def cogLayerMetadataFormat[K: JsonFormat] =
    new RootJsonFormat[COGLayerMetadata[K]] {
      def write(metadata: COGLayerMetadata[K]) =
        JsObject(
          "cellType" -> metadata.cellType.toJson,
          "zoomRanges" -> JsArray(metadata.zoomRanges.map(_.toJson)),
          "layoutScheme" -> metadata.layoutScheme.toJson,
          "extent" -> metadata.extent.toJson,
          "crs" -> metadata.crs.toJson
        )

      def read(value: JsValue): COGLayerMetadata[K] =
        value.asJsObject.getFields("cellType", "zoomRanges", "layoutScheme", "extent", "crs", "bounds") match {
          case Seq(cellType, JsArray(zoomRanges), layoutScheme, extent, crs, bounds) =>
            COGLayerMetadata(
              cellType.convertTo[CellType],
              zoomRanges.map(_.convertTo[(ZoomRange, KeyBounds[K])]).toVector,
              layoutScheme.convertTo[ZoomedLayoutScheme],
              extent.convertTo[Extent],
              crs.convertTo[CRS]
            )
          case _ =>
            throw new DeserializationException("COGLayerMetadata expected")
        }
    }
}
