package demo

import geotrellis._
import geotrellis.data._
import geotrellis.data.png._
import geotrellis.statistics._
import geotrellis.raster.op._

/*
 * Attempts to generate two rasters that are overlayed on top of each other.
 */
case class RenderOverlayPng(r:Op[Raster], colorBreaks:Op[ColorBreaks], h:Op[Histogram], rAlpha:Op[Double],
							r2:Op[Raster], colorBreaks2:Op[ColorBreaks], h2:Op[Histogram], r2Alpha:Op[Double],  
							noDataColor:Op[Int])
extends Operation[Array[Byte]] {
  def _run(context:Context) = runAsync(List('init, r, colorBreaks, h, rAlpha, r2, colorBreaks2, h2, r2Alpha, noDataColor))
  
  val nextSteps:Steps = {
    case 'init :: (r:Raster) :: (colorBreaks:ColorBreaks) :: (h:Histogram) :: (a:Double) 
               :: (r2:Raster) :: (colorBreaks2:ColorBreaks) :: (h2:Histogram) :: (a2:Double) 
               :: (noDataColor:Int) :: Nil => init(r, colorBreaks, h, a, r2, colorBreaks2, h2, a2, noDataColor)
    case 'finish :: (r:Raster) :: (renderer:Renderer) :: Nil => finish(r, renderer)
  }
  
  def init(r:Raster, colorBreaks:ColorBreaks, h:Histogram, a:Double, 
           r2:Raster, colorBreaks2:ColorBreaks, h2:Histogram, a2:Double,
           noDataColor:Int) : StepOutput[Array[Byte]] = {
    val rRendered = render(r, colorBreaks, h, noDataColor)
    val r2Rendered = render(r2, colorBreaks2, h2, noDataColor)
    
    runAsync('finish :: Blend(rRendered._1, a, r2Rendered._1, a2) :: r2Rendered._2 :: Nil)
  }
  
  def finish(r:Raster, renderer:Renderer) : Result[Array[Byte]] = {
    val bytes = new Encoder(renderer.settings).writeByteArray(r)
    Result(bytes)
  }
  
  def render(r:Raster, colorBreaks:ColorBreaks, h:Histogram, noDataColor:Int) = {
    val breaks = colorBreaks.limits
    val colors = colorBreaks.colors
    val renderer = Renderer(breaks, colors, h, noDataColor)
    (renderer.render(r), renderer)
  }
  
}