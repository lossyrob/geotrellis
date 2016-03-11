# GeoTrellis Spark Raster Operations

In `geotrellis.spark` we represent a raster layer as a distributed
collection of non-overlapping tiles indexed by keys according to some
`TileLayout`. For instance a spatial raster is represented as
`RDD[(GridKey, Tile)]` where `GridKey(col: Int, row: Int)` from
a `TileLayout`. In this setup we represent operations between raster
layers as a join.

## Metadata

A previously tiled and saved GeoTrellis Raster RDD read in through an
instance of geotrellis.spark.io.LayerReader will be mixed in with the
Metadata[LayerMetadata] trait. This metadata describes the TileLayout
used by the layer, the extent it covers, the CRS of its projection,
and what the CellType of each tile is. This metadata allows you to
verify that you are working with compatible layers.

```scala
import org.apache.spark._
import org.apache.spark.rdd._

import geotrellis.raster._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._

implicit val sc: SparkContext = ???

val reader : S3LayerReader[GridKey, Tile, LayerMetadata[GridKey]] =
  S3LayerReader.spatial("bucket", "prefix")

def getLayerId(idx: Int): LayerId = ???

val rdd1 =
  reader.read(getLayerId(1))

val rdd2: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  reader.read(getLayerId(2))

val rdd3: TileLayerRDD[SpaitalKey] =
  reader.read(getLayerId(3))
```

## Operations

GeoTrellis provides API for interaction with RDDs of tiles as a single
unit. We follow a pattern of providing symbolic methods where their
meaning if obvious and explicit method names in all cases.

```scala
import geotrellis.spark.op.local._

 rdd1 + 1           // add 1 to every cell in the tiled raster
 rdd1 localAdd 1    // explicit method name for above operation
 rdd1 + rdd2        // do a cell wise local addition between two rasters
 rdd1 localAdd rdd2 // explicit method name for above operation
 List(rdd1, rdd2, rdd3).localAdd
 // all results are of type RDD[(GridKey, Tile)]
```

Other supported operations can been seen `geotrellis.spark.op._`
package and it's sub-packages.

In order to provide this concise and intuitive syntax for map
algebra operations between two layers some assumptions need to
be made regarding the mechanics of the join. Specifically by
default GeoTrellis will use spark implementation of inner join
deferring to spark to producer appropriate partitioner for the
result. Thus if two layers being operated on are not aligned the
result of the operation will contain only the intersecting tiles.

### Explicit Joins

In cases where it is important to control the type of join a more
explicit manner is required. We make direct call to
`geotrellis.raster.op.local.Add` object to perform per tile operations.

Because all binary operations must have the shape of `(V, V) => R`
we provide an extension method on `RDD[(K, (V, V))]` that decomposes
the tuple resulting from the join and uses it to call a function
taking two arguments.


```scala
import geotrellis.raster.op.local._

// using spark API
rdd1.join(rdd2).mapValues { case (tile1: Tile, tile2: Tile) => Add(tile1, tile2) }

// using GeoTrellis method extensions
rdd1.join(rdd2).combineValues(Add(_, _))
```

#### Left Join

Another reason to want to control a join is to perform an update of a
larger layer with a smaller layer, performing an operation where two
intersect and capturing resulting values.

This case is captured by a left outer join. If the right-side of the
join row tuple is `None` we return the left-side tile unchanged.
Consequently the extension method `updateValues` will only accept
operations with signature of `(V, V) => V`.


```scala
// using spark API
rdd1.leftOuterJoin(rdd2).mapValues { case (tile1: Tile, optionTile: Option[Tile]) =>
  optionTile.fold(tile1)(Add(tile1, _))
}

// using GeoTrellis method extensions
rdd1.leftOuterJoin(rdd2).updateValues(Add(_, _))
```

#### Spatial Join

Given that we know the key bounds of our RDD, from accompanying
`LayerMetadata`, before performing the join we may use a spark
`Partitioner` that performs space partitioning. Such a partitioner
has a number of benefits over standard `HashPartitioenr`:

- Scales the number of partitions with the number of records in the RDD
- Produces partitions with spatial locality which allow:
- Faster focal operations
- Shuffle free joins with other spatially partitioned RDDs
- Efficient spatial region filtering

Because the partitioner requires ability to extract `Bounds` of the
original RDD from it's `Metadata` it is able to provide the `Bounds`
of the join result. Since the result of a join may be empty the user
must match on the resulting `Bounds` object to find out if it's
`EmptyBounds` or `KeyBounds[GridKey]`.

```scala
import geotrellis.spark.partitioner._

val joinRes: RDD[(GridKey, (Tile, Tile))] with Metadata[Bounds[GridKey]] =
  rdd1.spatialJoin(rdd2)

val leftJoinRes: RDD[(GridKey, (Tile, Option[Tile])] with Metadata[Bounds[GridKey]] =
  rdd1.spatialLeftOuterJoin(rdd2)
```

## Manipulating Metadata

Metadata is provided when loading a layer from a GeoTrellis layer
reader and is required when writing a layer through a GeoTrellis
layer writer. The user bears responsibility that it is preserved and
remains consistent through transformations if such behavior is desired.

The concrete implementation of `RDD[(K, V)] with Metadata[M]` signature
in GeoTrellis is `ContextRDD[K, V, M]`

```scala
val rdd: RDD[(GridKey, Tile)] = rdd1 localAdd rdd2
val rddWithContext: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  ContextRDD(rdd, rdd1.metadata)
```

### Preserving Metadata Through Operations

There are extension methods in `RDD[(K, V]] with Metadata[M]` that
allow either changing rdd while preserving metadata or changing
metadata while preserving the rdd.

```scala

// .withContext preserves the RDD context, the Metadata
val rddWithContext1: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  rdd1.withContext { _ localAdd rdd2 }

val rddWithContext2: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  rdd1.withContext { _ localAdd rdd2 localAdd rdd3 }


// .mapContext allows to chain changing Metadata after an operation
// example: localEqual will produce tiles with CellType of TypeBit
val rddWithContext3: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  rdd1
    .withContext { _ localEqual 123 }
    .mapContext { rmd: LayerMetadata => rmd.copy(cellType = TypeBit) }
```

### Preserving Metadata Through Spatial Joins

Since spatial joins produce metadata, in contrast to vanilla spark
joins, we must use `.withContext` wrapper at every transformation in
order to allow the updated `Bounds` to flow to the end where they can be used.

For instance lets assume we wrote `updateLayout` that combines
`Bounds[GridKey]` and `LayoutDefinition` from `LayerMetadata`
to produce an RDD with updated, smaller `TileLayout`.

```scala
def updateLayout(md: LayerMetadata, bounds: Bounds[GridKey]): LayerMetadata = ???

val rddWithContext: RDD[(GridKey, Tile)] with Metadata[LayerMetadata] =
  rdd1
    .spatialJoin(rdd2).withContext { _.combineValues(Add(_, _)) }
    .spatialJoin(rdd3).withContext { _.combineValues(Add(_, _)) }
    .mapContext{ bounds => updateLayout(rdd1.metadata, bounds) }
```
