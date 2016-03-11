package geotrellis.spark.io

import geotrellis.raster.{GridBounds, Tile}
import geotrellis.spark._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.json._
import geotrellis.vector.Extent

trait AllOnesTestTileTests { self: PersistenceSpec[GridKey, Tile, TileLayerMetadata[GridKey]] =>

  val bounds1 = GridBounds(1,1,3,3)
  val bounds2 = GridBounds(4,5,6,6)

  for(PersistenceSpecDefinition(keyIndexMethodName, _, layerIds) <- specLayerIds) {
    val layerId = layerIds.layerId
    val query = reader.query[GridKey, Tile, TileLayerMetadata[GridKey]](layerId)

    describe(s"AllOnes query tests for  $keyIndexMethodName") {
      it("filters past layout bounds") {
        query.where(Intersects(GridBounds(6, 2, 7, 3))).toRDD.keys.collect() should
        contain theSameElementsAs Array(GridKey(6, 3), GridKey(6, 2))
      }

      it("query inside layer bounds") {
        val actual = query.where(Intersects(bounds1)).toRDD.keys.collect()
        val expected = for ((x, y) <- bounds1.coords) yield GridKey(x, y)

        if (expected.diff(actual).nonEmpty)
          info(s"missing: ${(expected diff actual).toList}")
        if (actual.diff(expected).nonEmpty)
          info(s"unwanted: ${(actual diff expected).toList}")

        actual should contain theSameElementsAs expected
      }

      it("query outside of layer bounds") {
        query.where(Intersects(GridBounds(10, 10, 15, 15))).toRDD.collect() should be(empty)
      }

      it("disjoint query on space") {
        val actual = query.where(Intersects(bounds1) or Intersects(bounds2)).toRDD.keys.collect()
        val expected = for ((x, y) <- bounds1.coords ++ bounds2.coords) yield GridKey(x, y)

        if (expected.diff(actual).nonEmpty)
          info(s"missing: ${(expected diff actual).toList}")
        if (actual.diff(expected).nonEmpty)
          info(s"unwanted: ${(actual diff expected).toList}")

        actual should contain theSameElementsAs expected
      }

      it("should filter by extent") {
        val extent = Extent(-10, -10, 10, 10) // this should intersect the four central tiles in 8x8 layout
        query.where(Intersects(extent)).toRDD.keys.collect() should
        contain theSameElementsAs {
          for ((col, row) <- GridBounds(3, 3, 4, 4).coords) yield GridKey(col, row)
        }
      }
    }
  }
}
