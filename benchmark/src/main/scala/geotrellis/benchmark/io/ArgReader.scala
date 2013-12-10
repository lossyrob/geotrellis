package geotrellis.benchmark.io

import geotrellis._
import geotrellis.raster._
import geotrellis.data._

class ArgReader(path:String) extends FileReader(path) {
  def makeReadState(d:Either[String, Array[Byte]],
                    rasterType:RasterType,
                    rasterExtent:RasterExtent,
                    re:RasterExtent): ReadState = rasterType match {
    case TypeBit => new Int1ReadState(d, rasterExtent, re)
    case TypeByte => new Int8ReadState(d, rasterExtent, re)
    case TypeShort => new Int16ReadState(d, rasterExtent, re)
    case TypeInt => new Int32ReadState(d, rasterExtent, re)
    case TypeFloat => new Float32ReadState(d, rasterExtent, re)
    case TypeDouble => new Float64ReadState(d, rasterExtent, re)
    case t => sys.error("datatype %s is not supported" format t)
  }

  def readStateFromCache(b:Array[Byte], 
                         rasterType:RasterType, 
                         rasterExtent:RasterExtent, 
                         targetExtent:RasterExtent) = {
    makeReadState(Right(b), rasterType, rasterExtent, targetExtent)
  }

  def readStateFromPath(rasterType:RasterType, 
                        rasterExtent:RasterExtent,
                        targetExtent:RasterExtent):ReadState = {
    makeReadState(Left(path),rasterType,rasterExtent, targetExtent)
  }
}

