package geotrellis.gdal

import geotrellis.gdal.io.hadoop._
import geotrellis.proj4.LatLng
import geotrellis.spark._
import geotrellis.spark.ingest._
import geotrellis.spark.tiling._

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime
import org.scalatest._


class IngestSpec extends FunSpec
    with Matchers
    with OnlyIfGdalInstalled
    with TestEnvironment
{

  describe("Ingest") {
    ifGdalInstalled {
      val expectedKeys = List(
        GridTimeKey(GridKey(1,1),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(2,0),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(2,1),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(0,0),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(2,1),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(2,1),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(0,1),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(0,1),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(0,1),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(1,0),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(1,0),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(0,0),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(1,0),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(1,1),TimeKey(DateTime.parse("2006-01-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(1,1),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(2,0),TimeKey(DateTime.parse("2006-02-15T00:00:00.000Z"))),
        GridTimeKey(GridKey(2,0),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z"))),
        GridTimeKey(GridKey(0,0),TimeKey(DateTime.parse("2006-03-16T12:00:00.000Z")))
      )

      it("should ingest time-band NetCDF") {
        val source = sc.netCdfRDD(new Path(inputHome, "ipcc-access1-tasmin.nc"))
        Ingest[TimeProjectedExtent, GridTimeKey](source, LatLng, FloatingLayoutScheme(256)){ (rdd, level) =>
          val ingestKeys = rdd.keys.collect()
          info(ingestKeys.toList.toString)
          ingestKeys should contain theSameElementsAs expectedKeys
        }
      }

      it("should ingest time-band NetCDF in stages") {
        val source = sc.netCdfRDD(new Path(inputHome, "ipcc-access1-tasmin.nc"))
        val (zoom, rmd) = source.collectMetadata[GridTimeKey](LatLng, FloatingLayoutScheme(256))
        val tiled = source.cutTiles[GridTimeKey](rmd)
        val ingestKeys = tiled.keys.collect()
        ingestKeys should contain theSameElementsAs expectedKeys
      }
    }
  }
}
