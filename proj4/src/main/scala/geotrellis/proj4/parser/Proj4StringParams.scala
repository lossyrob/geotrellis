package geotrellis.proj4.parser

import geotrellis.proj4.InvalidValueException
import geotrellis.proj4.datum.Ellipsoid
import geotrellis.proj4.proj.{ProjectionBuilder, ProjectionType}
import geotrellis.proj4.units._
import geotrellis.proj4.util.ProjectionMath._

import monocle.syntax._

object Proj4StringParams {
  import ProjectionParams._

  private def tryParseInt(s: String) = try {
    Some(s.toInt)
  } catch {
    case e: NumberFormatException => None
  }

  private def tryParseDouble(s: String) = try {
    Some(s.toDouble)
  } catch {
    case e: NumberFormatException => None
  }

  private val format = new AngleFormat(AngleFormat.ddmmssPattern, true)

  def tryParseAngle(s: String) = try {
    Some(format.parse(s, null).doubleValue)
  } catch {
    case ex: Exception => None
  }

  def apply(params: Map[String, String], ellipsoid: Ellipsoid) {

    def tryGetInt(key: String) = params.get(key) match {
      case Some(s) => tryParseInt(s)
      case None => None
    }

    def tryGetDouble(key: String) = params.get(key) match {
      case Some(s) => tryParseDouble(s)
      case None => None
    }

    def tryGetAngle(key: String) = params.get(key) match {
      case Some(s) => tryParseAngle(s)
      case None => None
    }

    val alpha = tryGetDouble(Proj4Keyword.alpha) match {
      case Some(a) => DTR * a
      case None => Double.NaN
    }

    val lonc = tryGetDouble(Proj4Keyword.lonc) match {
      case Some(l) => DTR * l
      case None => Double.NaN
    }

    val projectionLatitude = tryGetAngle(Proj4Keyword.lat_0) match {
      case Some(a) => DTR * a
      case None => 0.0
    }

    val projectionLongitude = tryGetAngle(Proj4Keyword.lon_0) match {
      case Some(a) => DTR * a
      case None => 0.0
    }

    val projectionLatitude1 = tryGetAngle(Proj4Keyword.lat_1) match {
      case Some(a) => DTR * a
      case None => 0.0
    }

    val projectionLatitude2 = tryGetAngle(Proj4Keyword.lat_2) match {
      case Some(a) => DTR * a
      case None => 0.0
    }

    val trueScaleLatitude = tryGetAngle(Proj4Keyword.lat_ts) match {
      case Some(a) => DTR * a
      case None => 0.0
    }

    val falseEasting = tryGetDouble(Proj4Keyword.x_0).getOrElse(0.0)

    val falseNorthing = tryGetDouble(Proj4Keyword.y_0).getOrElse(0.0)

    val scaleFactor = tryGetDouble(Proj4Keyword.k_0) match {
      case Some(sf) => sf
      case None => tryGetDouble(Proj4Keyword.k).getOrElse(1.0)
    }

    val unit = params.get(Proj4Keyword.units) match {
      case Some(code) => Units.findUnits(code)
      case None => Units.METRES
    }

    val fromMetres = tryGetDouble(Proj4Keyword.to_meter) match {
      case Some(d) => d
      case None => 1 / unit.value
    }

    val southernHemisphere = params.contains(Proj4Keyword.south)

    val utmZone = params.get(Proj4Keyword.proj) match {
      case Some("tmerc") | Some("utm") => tryGetInt(Proj4Keyword.zone)
      case _ => None
    }

    var pp = ProjectionParams(projectionType = projection)

    pp = pp |-> _alpha set(alpha)

    pp = pp |-> _lonc set(lonc)

    pp = pp |-> _projectionLatitude set(projectionLatitude)

    pp = pp |-> _projectionLongitude set(projectionLongitude)

    pp = pp |-> _projectionLatitude1 set(projectionLatitude1)

    pp = pp |-> _projectionLatitude2 set(projectionLatitude2)

    pp = pp |-> _trueScaleLatitude set(trueScaleLatitude)

    pp = pp |-> _falseEasting set(falseEasting)

    pp = pp |-> _falseNorthing set(falseNorthing)

    pp = pp |-> _scaleFactor set(scaleFactor)

    pp = pp |-> _unit set(unit)

    pp = pp |-> _fromMetres set(fromMetres)

    pp = pp |-> _southernHemisphere set(southernHemisphere)

    pp |-> _utmZone set(utmZone)

    pp |-> _ellipsoid set(ellipsoid)

    pp
  }
}
