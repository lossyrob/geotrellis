package demo

import geotrellis._
import geotrellis.raster.op.local._

object Blend {
  def apply(x:Op[Raster], xAlpha:Double, y:Op[Raster], yAlpha:Double) = BlendRaster(x,xAlpha,y,yAlpha)
 }

/**
 * Blend two color rasters together based on the provided alpha values. 
 */
case class BlendRaster(r1:Op[Raster], r1Alpha:Double, r2:Op[Raster], r2Alpha:Double) extends BinaryLocal {
  def handle(z1:Int, z2:Int) = {
    if (z1 == NODATA) z2
    else if (z2 == NODATA) z1
    else {
    	handleDouble(z1.toDouble,z2.toDouble).toInt  
    }
  }

  def handleDouble(z1:Double, z2:Double) = {
    if (java.lang.Double.isNaN(z1)) z2
    else if (java.lang.Double.isNaN(z2)) z1
    else {
      
    	val i1 = z1.toInt
    	val i2 = z2.toInt
    	
    	val r = (r1Alpha * (i1 & 0xFF0000)).toInt + ((1 - r1Alpha) * r2Alpha * (i2 & 0xFF0000)).toInt
    	val g = (r1Alpha * (i1 & 0x00FF00)).toInt + ((1 - r1Alpha) * r2Alpha * (i2 & 0x00FF00)).toInt
    	val b = (r1Alpha * (i1 & 0x0000FF)).toInt + ((1 - r1Alpha) * r2Alpha * (i2 & 0x0000FF)).toInt
    	(r+g+b).toDouble
    }
  }
}
