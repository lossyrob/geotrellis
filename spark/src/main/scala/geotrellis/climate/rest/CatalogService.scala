package climate.rest



import akka.actor.ActorSystem
import com.quantifind.sumac.{ ArgApp, ArgMain }

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.Nodata
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.cmd.args.{ AccumuloArgs, SparkArgs, HadoopArgs }
import geotrellis.spark.io.accumulo._
import geotrellis.spark.json._
import geotrellis.spark.tiling._
import geotrellis.vector.reproject._

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.joda.time.DateTime
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http.{ AllOrigins, MediaTypes }
import spray.http.{ HttpMethods, HttpMethod, HttpResponse, AllOrigins }
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing._
import spray.routing.SimpleRoutingApp

trait CORSSupport { self: HttpService =>
  val corsHeaders = List(`Access-Control-Allow-Origin`(AllOrigins),
    `Access-Control-Allow-Methods`(GET, POST, OPTIONS, DELETE),
    `Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"))

  def cors: Directive0 = {
    val rh = implicitly[RejectionHandler]
    respondWithHeaders(corsHeaders) & handleRejections(rh)
  }
}

class TmsArgs extends SparkArgs with AccumuloArgs with HadoopArgs

/**
 * Catalog and TMS service for TimeRaster layers only
 * This is intended to exercise the machinery more than being a serious service.
 */
object CatalogService extends ArgApp[TmsArgs] with SimpleRoutingApp with SprayJsonSupport with CORSSupport {
  implicit val system = ActorSystem("spray-system")
  implicit val sc = argHolder.sparkContext("Catalog Service") // for geotrellis

  val accumulo = AccumuloInstance(argHolder.instance, argHolder.zookeeper,
    argHolder.user, new PasswordToken(argHolder.password))
  val catalog = accumulo.catalog

  /** Simple route to test responsiveness of service. */
  val pingPong = path("ping")(complete("pong"))

  /** Server out TMS tiles for some layer */
  def tmsRoute =
    pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (layer, zoom, x, y) =>
      parameters('time) { time =>
        val dt = DateTime.parse(time)
        println(layer, zoom, x, y, dt)

        val rdd = catalog.load[SpaceTimeKey](LayerId(layer, zoom), FilterSet[SpaceTimeKey]()
          withFilter SpaceFilter(GridBounds(x, y, x, y))
          withFilter TimeFilter(dt, dt))

        respondWithMediaType(MediaTypes.`image/png`) {
          complete {
            future {
              val tile = rdd.get.first().tile
              val breaks = Array(0, 1, 2, 3, 4, 5, 6)
              tile.renderPng(ColorRamps.BlueToOrange).bytes
            }
          }
        }
      }
    }

  def catalogRoute = cors {
    path("") {
      get {
        // get the entire catalog
        complete {
          import DefaultJsonProtocol._
          accumulo.metaDataCatalog.fetchAll.mapValues(_._1).toSeq.map {
            case (layer, md) =>              
              val center = md.extent.reproject(md.crs, LatLng).center
              var bands = {
                val GridBounds(col, row, _, _) = md.mapTransform(md.extent)
                val filters = new FilterSet[SpaceTimeKey]() withFilter SpaceFilter(GridBounds(col, row, col, row))
                catalog.load(layer, filters).map { // into Try
                  _.map { case (key, tile) => key.temporalKey.time.toString }
                }
              }.get.collect
              JsObject(
                "layer" -> layer.toJson,
                "metaData" -> md.toJson,
                "center" -> List(center.x, center.y).toJson,
                "bands" -> JsObject("time" -> bands.toJson))
          }
        }
      }
    }
  }
  def root = {
    pathPrefix("catalog") { catalogRoute } ~
      pathPrefix("tms") { tmsRoute }
  }

  startServer(interface = "localhost", port = 8080) {
    get(pingPong ~ root)
  }
}