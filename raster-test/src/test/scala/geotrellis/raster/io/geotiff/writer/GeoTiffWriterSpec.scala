/*
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.raster.io.geotiff.writer

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader._

import geotrellis.vector.Extent

import geotrellis.proj4.CRS
import geotrellis.proj4.LatLng

import geotrellis.raster.testkit._

import java.io._

import org.scalatest._

class GeoTiffWriterSpec extends FunSpec
    with Matchers
    with BeforeAndAfterAll
    with RasterMatchers
    with TileBuilders
    with GeoTiffTestUtils {

  override def afterAll = purge

  private val testCRS = CRS.fromName("EPSG:3857")

  describe ("writing GeoTiffs without errors and with correct tiles, crs and extent") {
    val temp = File.createTempFile("geotiff-writer", ".tif"); 
    val path = temp.getPath()

    it("should write GeoTiff with tags") {
      val geoTiff = MultibandGeoTiff(geoTiffPath("multi-tag.tif"))
      GeoTiffWriter.write(geoTiff, path)

      addToPurge(path)

      val actual = MultibandGeoTiff(path).tags
      val expected = geoTiff.tags

      actual should be (expected)
    }

    it("should write web mercator correctly") {
      val geoTiff = SinglebandGeoTiff(geoTiffPath("ndvi-web-mercator.tif"))

      addToPurge(path)
      geoTiff.write(path)
      val actualCRS = SinglebandGeoTiff(path).crs

      actualCRS.epsgCode should be (geoTiff.crs.epsgCode)
    }

    it("should write NY State Plane correctly") {
      val geoTiff = SinglebandGeoTiff(geoTiffPath("ny-state-plane.tif"))

      addToPurge(path)
      geoTiff.write(path)
      val actualCRS = SinglebandGeoTiff(path).crs

      actualCRS.epsgCode should be (geoTiff.crs.epsgCode)
    }

    it ("should write Polar stereographic correctly") {
      val geoTiff = SinglebandGeoTiff(geoTiffPath("alaska-polar-3572.tif"))

      addToPurge(path)
      geoTiff.write(path)
      val actualCRS = SinglebandGeoTiff(path).crs

      actualCRS.epsgCode should be (geoTiff.crs.epsgCode)
    }

    it ("should write floating point rasters correct") {
      val e = Extent(100.0, 400.0, 120.0, 420.0)
      val t = DoubleArrayTile(Array(11.0, 22.0, 33.0, 44.0), 2, 2)

      val geoTiff = SinglebandGeoTiff(t, e, testCRS, Tags.empty, GeoTiffOptions.DEFAULT)

      GeoTiffWriter.write(geoTiff, path)

      addToPurge(path)

      val SinglebandGeoTiff(tile, extent, crs, _) = SinglebandGeoTiff(path)

      extent should equal (e)
      crs should equal (testCRS)
      assertEqual(tile, t)
    }

    it ("should read write raster correctly") {
      val geoTiff = SinglebandGeoTiff.compressed(geoTiffPath("econic_zlib_tiled_bandint_wm.tif"))
      val projectedRaster = geoTiff.projectedRaster
      val ProjectedRaster(Raster(tile, extent), crs) = projectedRaster.reproject(LatLng)
      val reprojGeoTiff = SinglebandGeoTiff(tile, extent, crs, geoTiff.tags, geoTiff.options)

      GeoTiffWriter.write(reprojGeoTiff, path)

      addToPurge(path)

      val SinglebandGeoTiff(actualTile, actualExtent, actualCrs, _) = SinglebandGeoTiff(path)

      actualExtent should equal (extent)
      crs should equal (LatLng)
      assertEqual(actualTile, tile)
    }

    it ("should read write multibandraster correctly") {
      val geoTiff = MultibandGeoTiff(geoTiffPath("3bands/int32/3bands-striped-pixel.tif"))

      GeoTiffWriter.write(geoTiff, path)

      addToPurge(path)

      val gt = MultibandGeoTiff(path)

      gt.extent should equal (geoTiff.extent)
      gt.crs should equal (geoTiff.crs)
      gt.tile.bandCount should equal (geoTiff.tile.bandCount)
      for(i <- 0 until gt.tile.bandCount) {
        val actualBand = gt.band(i)
        val expectedBand = geoTiff.band(i)

        assertEqual(actualBand, expectedBand)
      }
    }

    it ("should write hand made multiband and read back correctly") {
      val tile =
        ArrayMultibandTile(
          positiveIntegerRaster,
          positiveIntegerRaster.map(_ * 100),
          positiveIntegerRaster.map(_ * 10000)
        )

      val geoTiff = MultibandGeoTiff(tile, Extent(0.0, 0.0, 1000.0, 1000.0), LatLng)

      GeoTiffWriter.write(geoTiff, path)

      addToPurge(path)

      val gt = MultibandGeoTiff(path)

      gt.extent should equal (geoTiff.extent)
      gt.crs should equal (geoTiff.crs)
      gt.tile.bandCount should equal (tile.bandCount)
      for(i <- 0 until gt.tile.bandCount) {
        val actualBand = gt.band(i)
        val expectedBand = tile.band(i)

        assertEqual(actualBand, expectedBand)
      }
    }

    it("should write a GeoTiff to byte array") {
      val tile =
        ArrayMultibandTile(
          positiveIntegerRaster,
          positiveIntegerRaster.map(_ * 100),
          positiveIntegerRaster.map(_ * 10000)
        )

      val geoTiff = MultibandGeoTiff(tile, Extent(0.0, 0.0, 1000.0, 1000.0), LatLng)

      val bytes = GeoTiffWriter.write(geoTiff)

      val gt = MultibandGeoTiff(bytes)

      gt.extent should equal (geoTiff.extent)
      gt.crs should equal (geoTiff.crs)
      gt.tile.bandCount should equal (tile.bandCount)
      for(i <- 0 until gt.tile.bandCount) {
        val actualBand = gt.band(i)
        val expectedBand = tile.band(i)

        assertEqual(actualBand, expectedBand)
      }
    }
  }
}
