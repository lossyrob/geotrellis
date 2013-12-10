package geotrellis.process

import geotrellis._
import geotrellis.raster._
import geotrellis.util._

import dispatch.classic._

import com.typesafe.config.Config

object ArgUrlRasterLayerBuilder
extends RasterLayerBuilder {
  def apply(ds:Option[String],jsonPath:String, json:Config):Option[RasterLayer] = {
    val url = 
      if(json.hasPath("url")) {
        json.getString("url")
      } else {
        System.err.println(s"[ERROR] 'argurl' type rasters must have 'url' field in json.")
        System.err.println("[ERROR]   Skipping this raster layer...")
        return None
      }

    val cols = json.getInt("cols")
    val rows = json.getInt("rows")

    val (cellWidth,cellHeight) = getCellWidthAndHeight(json)
    val rasterExtent = RasterExtent(getExtent(json), cellWidth, cellHeight, cols, rows)

    val info = 
      RasterLayerInfo(
        LayerId(ds,getName(json)),
        getRasterType(json),
        rasterExtent,
        getEpsg(json),
        getXskew(json),
        getYskew(json),
        getCacheFlag(json)
      )

    Some(new ArgUrlRasterLayer(info,url))
  }
}

class ArgUrlRasterLayer(info:RasterLayerInfo, rasterUrl:String) 
extends UntiledRasterLayer(info) {
  def getRaster(targetExtent:Option[RasterExtent]) =
    if(isCached) {
      getCache.lookup[Array[Byte]](info.id.toString) match {
        case Some(bytes) =>
          fromBytes(bytes, targetExtent)
        case None =>
          sys.error("Cache problem: Layer thinks it's cached but it is in fact not cached.")
      }
    } else {
      fromBytes(getBytes, targetExtent)
    }

  def getBytes = {
    val size = info.rasterExtent.cols * info.rasterExtent.rows * (info.rasterType.bits / 8)
    val result = Array.ofDim[Byte](size)
    val h = new Http()
    h(url(rasterUrl) >>  { (stream,charset) =>
      var bytesRead = 1
      while (bytesRead > 0) { 
        bytesRead = stream.read(result, bytesRead-1, size)
      }
    })
    h.shutdown
    result
  }

  def cache(c:Cache[String]) = 
        c.insert(info.id.toString, getBytes)

  private def fromBytes(arr:Array[Byte],target:Option[RasterExtent]) = {
    val data = 
      RasterData.fromArrayByte(arr,info.rasterType,info.rasterExtent.cols,info.rasterExtent.rows)
    target match {
      case Some(re) =>
        Raster(data.warp(info.rasterExtent,re),re)
      case None =>
        Raster(data,info.rasterExtent)
    }
  }
}

