package demo

import javax.servlet.http.{HttpServletRequest}
import javax.ws.rs.{GET, Path, DefaultValue, PathParam, QueryParam}
import javax.ws.rs.core.{Response, Context}

import geotrellis._
import geotrellis.data.{ColorBreaks}
import geotrellis.statistics.{Histogram}
import geotrellis.process.{Server}
import geotrellis.Implicits._

import geotrellis.raster.op._
import geotrellis.rest.op._
import geotrellis.statistics.op._
import geotrellis.io._

object HelensUtil {
  def getNormalizedBefore() = {
    // Load up elevation data from before the volcano exploded
    val beforeOp = io.LoadRaster("helens-before")
    
    // Clean the data a bit by removing negative values
    val rOp:Op[Raster] = local.IfCell(beforeOp, z => z < 0, 0)
    
    // Normalized to Mt. St. Helen's height before explosion
    val rNormalizedOp = focal.Rescale(rOp, Literal((0,9677)))
    
    rNormalizedOp
  }
  
  def getNormalizedAfter() = {
    // Load up elevation data from after the volcano exploded
    val afterOp = io.LoadRaster("helens-after")
    
    // Clean the data a bit by removing negative values
    val rOp:Op[Raster] = local.IfCell(afterOp, z => z < 0, 0)
    
    // Normalized to Mt. St. Helen's height before explosion
    val rNormalizedOp = focal.Rescale(rOp, Literal((0,8366)))
    
    rNormalizedOp
  }
  
  def getDiff() = {
    val beforeOp = getNormalizedBefore()
    val afterOp = getNormalizedAfter()
    
    // We only care about decreases in elevation.
    val deltaOp = local.IfCell(beforeOp - afterOp, x => x < 0, 0)
    
    deltaOp
  }
}

@Path("/helens-diff")
class HelensDiff {
  @GET
  def get(@Context req:HttpServletRequest):Response = {
    get("ff000,0000ff","50",req)
  }
  
  @GET
  @Path("/shades/{shades}")
  def get(@PathParam("shades") shades:String,
          @Context req:HttpServletRequest):Response = {
    get("ff000,0000ff",shades,req)
  }
  
  @GET
  @Path("/palette/{palette}/shades/{shades}")
  def get(@PathParam("palette") palette:String,
          @PathParam("shades") shades:String,
          @Context req:HttpServletRequest):Response = {

    val deltaOp = HelensUtil.getDiff()
    
    // find the colors to use
    val paletteOp:Op[Array[Int]] = logic.ForEach(string.SplitOnComma(palette))(string.ParseHexInt(_))
    val numOp:Op[Int] = string.ParseInt(shades)
    val colorsOp:Op[Array[Int]] = stat.GetColorsFromPalette(paletteOp, numOp)
    
    // find the appropriate quantile class breaks to use for the delta
    val deltaHistogramOp:Op[Histogram] = stat.GetHistogram(deltaOp)
    val deltaBreaksOp:Op[ColorBreaks] = stat.GetColorBreaks(deltaHistogramOp, colorsOp)
    
    // render the png
    val pngOp:Op[Array[Byte]] = RenderPng(deltaOp, deltaBreaksOp, deltaHistogramOp, Literal(0))
    
    try {
      val img:Array[Byte] = Demo.server.run(pngOp)
      response("image/png")(img)
    } catch {
      case e => response("text/plain")(e.toString)
   }
  }
}

/*
 * This doesn't work as expected, but given enough love it could!
 */
@Path("/helens-overlay")
class HelensOverlay {
  @GET
  def get(@Context req:HttpServletRequest):Response = {
    get("0.9","1","ff0000,0000ff","100",req)
  }
  
  @GET
  @Path("/alpha1/{alpha1}/alpha2/{alpha2}/palette/{palette}")
  def get(@PathParam("alpha1") alpha1:String,
          @PathParam("alpha2") alpha2:String,
		  @Context req:HttpServletRequest):Response = {
     get(alpha1,alpha2,"ff0000,0000ff","100",req)
  }
  
  @GET
  @Path("/shades/{shades}")
  def get(@PathParam("shades") shades:String,
		  @Context req:HttpServletRequest):Response = {
     get("0.9","1","ff0000,0000ff",shades,req)
  }
  
  @GET
  @Path("/alpha1/{alpha1}/alpha2/{alpha2}/palette/{palette}/shades/{shades}")
  def get(@PathParam("alpha1") alpha1:String,
          @PathParam("alpha2") alpha2:String,
          @PathParam("palette") palette:String,
          @PathParam("shades") shades:String,
		  @Context req:HttpServletRequest):Response = {
    
    val deltaOp = HelensUtil.getDiff()
    val beforeOp = HelensUtil.getNormalizedBefore()
    
    // find the colors to use
    //val paletteOp:Op[Array[Int]] = logic.ForEach(string.SplitOnComma("00ffff,ff00ff"))(string.ParseHexInt(_))
    val paletteOp:Op[Array[Int]] = logic.ForEach(string.SplitOnComma(palette))(string.ParseHexInt(_))
    val numOp:Op[Int] = string.ParseInt(shades)
    val colorsOp:Op[Array[Int]] = stat.GetColorsFromPalette(paletteOp, numOp)
    
    // find the appropriate quantile class breaks to use for the delta
    val deltaHistogramOp:Op[Histogram] = stat.GetHistogram(deltaOp)
    val deltaBreaksOp:Op[ColorBreaks] = stat.GetColorBreaks(deltaHistogramOp, colorsOp)
    
    // find the appropriate quantile class breaks to use for the before picture
    val beforeHistogramOp:Op[Histogram] = stat.GetHistogram(beforeOp)
    val beforeBreaksOp:Op[ColorBreaks] = stat.GetColorBreaks(beforeHistogramOp, colorsOp)
    
    val alpha1Op = string.ParseDouble(alpha1)
    val alpha2Op = string.ParseDouble(alpha2)
    
    // render the png
    val pngOp:Op[Array[Byte]] = RenderOverlayPng(beforeOp, beforeBreaksOp, beforeHistogramOp, alpha1Op,
    							           deltaOp, deltaBreaksOp, deltaHistogramOp, alpha2Op,
    							           Literal(0))
    
    try {
      val img:Array[Byte] = Demo.server.run(pngOp)
      response("image/png")(img)
    } catch {
      case e => response("text/plain")(e.toString)
   }
  }
}