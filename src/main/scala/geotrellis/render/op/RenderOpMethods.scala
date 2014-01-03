package geotrellis.render.op

import geotrellis._
import geotrellis.render._
import geotrellis.source._

trait RenderOpMethods[+Repr <: RasterDS] { self: Repr =>
  def color(breaksToColors:Map[Int,Int]):RasterSource =
    color(breaksToColors,ColorMapOptions.Default)

  def color(breaksToColors:Map[Int,Int],options:ColorMapOptions):RasterSource =
    mapOp(ColorRaster(_,breaksToColors,options))

  def color(breaksToColors:Map[Double,Int])(implicit d:DI):RasterSource =
    color(breaksToColors,ColorMapOptions.Default)

  def color(breaksToColors:Map[Double,Int],options:ColorMapOptions)(implicit d:DI):RasterSource =
    mapOp(ColorRaster(_,breaksToColors,options))

  /** Generate a PNG from a raster of RGBA integer values.
    *
    * Use this operation when you have created a raster whose values are already
    * RGBA color values that you wish to render into a PNG. If you have a raster
    * with data that you wish to render, you should use RenderPng instead.
    *
    * An RGBA value is a 32 bit integer with 8 bits used for each component:
    * the first 8 bits are the red value (between 0 and 255), then green, blue,
    * and alpha (with 0 being transparent and 255 being opaque).
    */
  def renderPng():ValueSource[Png] =
    self.converge.mapOp(RenderPngRgba(_))

  def renderPng(colorRamp:ColorRamp):ValueSource[Png] = 
    self.converge.mapOp(SimpleRenderPng(_,colorRamp))

  def renderPng(colorBreaks:ColorBreaks):ValueSource[Png] = 
    renderPng(colorBreaks,0)

  def renderPng(colorBreaks:ColorBreaks,noDataColor:Int):ValueSource[Png] = 
    self.converge.mapOp(RenderPng(_,colorBreaks,noDataColor))

  def renderPng(ramp:ColorRamp, breaks:Array[Int]):ValueSource[Png] =
    renderPng(ColorBreaks.assign(breaks,ramp.toArray))

  def renderPng(colors:Array[Int]):ValueSource[Png] =
    self.converge.mapOp(SimpleRenderPng(_,colors))

  def renderPng(colors:Array[Int], numColors:Int):ValueSource[Png] =
    self.converge.mapOp(SimpleRenderPng(_,Color.chooseColors(colors,numColors)))
}
