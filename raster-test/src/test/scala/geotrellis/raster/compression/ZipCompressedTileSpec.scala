package geotrellis.raster.compression

import geotrellis.raster._
import geotrellis.raster.io.geotiff._

import geotrellis.raster.testkit._

import org.scalatest._

import spire.syntax.cfor._

class ZipCompressedTileSpec extends FunSpec
    with TileBuilders
    with RasterMatchers {

  describe("Zip Compressed Tiles") {

    it("should compress and decompress a custom tile with Zip correctly #1") {
      val tile = ArrayTile(
        Array(1, 2, 3, 4, 5, 6, 7, 8, 9),
        3,
        3
      )

      val compressedTile = tile.compress(Zip)

      println(s"Compression ratio for Zip: ${compressedTile.compressionRatio}")

      val decompressedTile = compressedTile.decompress

      tilesEqual(tile, decompressedTile)
    }

    it("should compress and decompress a custom tile with Zip correctly #2") {
      val tile = ArrayTile(
        Array(1, 1, 1, 1, 1, 1, 1, 1, 1),
        3,
        3
      )

      val compressedTile = tile.compress(Zip)

      println(s"Compression ratio for Zip: ${compressedTile.compressionRatio}")

      val decompressedTile = compressedTile.decompress

      tilesEqual(tile, decompressedTile)
    }

    it("should compress and decompress aspect.tif with Zip correctly") {
      val tile = SinglebandGeoTiff("raster-test/data/aspect.tif").tile

      val compressedTile = tile.compress(Zip)

      println(s"Compression ratio for Zip on aspect.tif: ${compressedTile.compressionRatio}")

      val decompressedTile = compressedTile.decompress

      tilesEqual(tile, decompressedTile)
    }

    it("should compress and decompress slope.tif with Zip correctly") {
      val tile = SinglebandGeoTiff("raster-test/data/slope.tif").tile

      val compressedTile = tile.compress(Zip)

      println(s"Compression ratio for Zip on slope.tif: ${compressedTile.compressionRatio}")

      val decompressedTile = compressedTile.decompress

      tilesEqual(tile, decompressedTile)
    }
  }
}
