/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.ingest

import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.io.hadoop._
import geotrellis.proj4._

import org.apache.hadoop.fs.Path
import org.scalatest._

class IngestSpec extends FunSpec
  with Matchers
  with TestEnvironment {
  describe("Ingest") {
    it("should read GeoTiff with overrided input CRS") {
      val source = HadoopGeoTiffRDD.spatial(new Path(inputHome, "all-ones.tif"), HadoopGeoTiffRDD.Options(crs = Some(CRS.fromEpsgCode(3857))))
//      val source = sc.hadoopGeoTiffRDD(new Path(inputHome, "all-ones.tif"), sc.defaultTiffExtensions, crs = "EPSG:3857")
      source.take(1).toList.map { case (k, _) => k.crs.proj4jCrs.getName }.head shouldEqual "EPSG:3857"
    }

    it("should ingest GeoTiff") {
      val source = sc.hadoopGeoTiffRDD(new Path(inputHome, "all-ones.tif"))
      Ingest[ProjectedExtent, SpatialKey](source, LatLng, ZoomedLayoutScheme(LatLng, 512)) { (rdd, zoom) =>
        zoom should be (10)
        rdd.filter(!_._2.isNoDataTile).count should be (8)
      }
    }

    it("should ingest GeoTiff with preset max zoom level") {
      val source = sc.hadoopGeoTiffRDD(new Path(inputHome, "all-ones.tif"))
      Ingest[ProjectedExtent, SpatialKey](source, LatLng, ZoomedLayoutScheme(LatLng, 512), maxZoom = Some(11)) { (rdd, zoom) =>
        zoom should be (11)
        rdd.filter(!_._2.isNoDataTile).count should be (18)
      }
    }
  }
}
