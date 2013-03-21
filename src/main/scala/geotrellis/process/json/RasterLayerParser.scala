package geotrellis.process.json

import org.codehaus.jackson._
import org.codehaus.jackson.JsonToken._
import org.codehaus.jackson.map._

import scala.collection.mutable

object RasterLayerParser {
  class RequiredField(f:JsonParser=>Unit) {
    var hit = false

    def set(parser:JsonParser) = { f(parser) ; hit = true }
  }

  val parserFactory = new MappingJsonFactory()

  def error(msg:String) = sys.error(s"Invalid json for arg file: $msg")

  def apply(json:String):RasterLayerRec = {
    var layer = ""
    var ltype = ""
    var datatype = ""
    var xmin = 0.0
    var xmax = 0.0
    var ymin = 0.0
    var ymax = 0.0
    var cols = 0
    var rows = 0
    var cellheight = 0.0
    var cellwidth = 0.0
    var epsg = 0
    var yskew = 0.0
    var xskew = 0.0

    val requiredFields = Map(
      ( "layer", new RequiredField(parser => layer = parser.getText())),
      ( "type", new RequiredField(parser => ltype = parser.getText())),
      ( "datatype", new RequiredField(parser => datatype = parser.getText())),
      ( "xmin", new RequiredField(parser => xmin = parser.getDoubleValue())),
      ( "xmax", new RequiredField(parser => xmax = parser.getDoubleValue())),
      ( "ymin", new RequiredField(parser => ymin = parser.getDoubleValue())),
      ( "ymax", new RequiredField(parser => ymax = parser.getDoubleValue())),
      ( "cols", new RequiredField(parser => cols = parser.getIntValue())),
      ( "rows", new RequiredField(parser => rows = parser.getIntValue())),
      ( "cellheight", new RequiredField(parser => cellheight = parser.getDoubleValue())),
      ( "cellwidth", new RequiredField(parser => cellwidth = parser.getDoubleValue())),
      ( "epsg", new RequiredField(parser => epsg = parser.getIntValue())),
      ( "yskew", new RequiredField(parser => yskew = parser.getDoubleValue())),
      ( "xskew", new RequiredField(parser => xskew = parser.getDoubleValue()))
    )

    val parser = parserFactory.createJsonParser(json)
    
    if(parser.nextToken() != START_OBJECT) 
      error("No data available.")
    else { 
      var token = parser.nextToken()

      while(token != null) {
        token match {
          case FIELD_NAME => {
            val name = parser.getCurrentName()
            if(requiredFields.contains(name)) {
              parser.nextToken()
              requiredFields(name).set(parser)
            } else {
              parser.nextToken() //Ignore
            }
          }
          case END_OBJECT => // Done
          case _ => error(s"Unexpected token $token.")
        }
        token = parser.nextToken()
      }
    }

    requiredFields.values.find(v => !v.hit) match {
      case Some(kv) =>
        val s = requiredFields.filter(kv => !kv._2.hit).map(kv => kv._1).reduceLeft((a,b) => s"$a, $b")
        error(s"Required fields not found: $s")
      case None =>
        RasterLayerRec(layer, ltype, datatype, xmin, xmax, ymin, ymax, 
                       cols, rows, cellheight, cellwidth, 
                       epsg, yskew, xskew)
    }
  }
}
