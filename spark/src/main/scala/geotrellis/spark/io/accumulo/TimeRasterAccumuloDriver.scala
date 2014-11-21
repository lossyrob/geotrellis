package geotrellis.spark.io.accumulo

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.tiling._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.data.{Range => ARange, Key, Value, Mutation}
import org.apache.accumulo.core.util.{Pair => JPair}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import scala.collection.JavaConversions._
import scala.collection.mutable

object TimeRasterAccumuloDriver extends AccumuloDriver[SpaceTimeKey] {
  def rowId(layerId: LayerId, col: Int, row: Int, time: DateTime) = new Text(s"${layerId.name}_${col}_${row}_${time.withZone(DateTimeZone.UTC).toString}_${layerId.zoom}")
  val rowIdRx = """(\d+)_(\d+)_([^_]+)_([^_]+)_(\d+)""".r // (zoom)_(SpatialKey.col)_(SpatialKey.row)

  /** Map rdd of indexed tiles to tuples of (table name, row mutation) */
  def encode(layerId: LayerId, raster: RasterRDD[SpaceTimeKey]): RDD[(Text, Mutation)] =
    raster.map {
      case (SpaceTimeKey(spatialKey, time), tile) =>
        val mutation = new Mutation(rowId(layerId, spatialKey.col, spatialKey.row, time))
        mutation.put(new Text(), new Text(),
          System.currentTimeMillis(), new Value(tile.toBytes()))
        (null, mutation)
    }

  /** Maps RDD of Accumulo specific Key, Value pairs to a tuple of (K, Tile) and wraps it in RasterRDD */
  def decode(rdd: RDD[(Key, Value)], metaData: RasterMetaData): RasterRDD[SpaceTimeKey] = {
    val tileRdd = rdd.map {
      case (key, value) =>
        val rowIdRx(col, row, timeString, layerName, zoom) = key.getRow.toString
        val spatialKey = SpatialKey(col.toInt, row.toInt)
        val time = DateTime.parse(timeString)
        val tile = ArrayTile.fromBytes(value.get, metaData.cellType, metaData.tileLayout.tileCols, metaData.tileLayout.tileRows)
        SpaceTimeKey(spatialKey, time) -> tile.asInstanceOf[Tile]
    }
    new RasterRDD(tileRdd, metaData)
  }

  def setZoomBounds(job: Job, layerId: LayerId): Unit = {
    val range = new ARange(
      new Text(s"${layerId.zoom}"),
      new Text(s"${layerId.zoom+1}")
    ) :: Nil
    InputFormatBase.setRanges(job, range)
  }

  def setFilters(job: Job, layerId: LayerId, filterSet: FilterSet[SpaceTimeKey]): Unit = {
    var tileBoundSet = false
    var bounds: GridBounds = null
    var from: DateTime = null
    var to: DateTime = null

    filterSet.filters.foreach {
      case SpaceFilter(filterBounds) =>
        tileBoundSet = true
	bounds = filterBounds

//        val ranges = 
//          for(row <- bounds.rowMin to bounds.rowMax) yield {
//            new ARange(rowId(layerId, bounds.colMin, row), rowId(layerId, bounds.colMax, row))
//          }

//        InputFormatBase.setRanges(job, ranges)
      case TimeFilter(startTime, endTime) =>
        from = startTime
        to = endTime
//        val from = new JPair(new Text(layerId.name), new Text(startTime.toString))
//        val to = new JPair(new Text(layerId.name), new Text(endTime.toString))

//        val props = 
//          Map(
//            "startBound" -> startTime.toString, 
//            "endBound" -> endTime.toString,
//            "startInclusive" -> "true",
//            "endInclusive" -> "true"
//          )

//        val iterator = 
//          new IteratorSetting(1, "TimeColumnFilter", "org.apache.accumulo.core.iterators.user.ColumnSliceFilter", props)
//        InputFormatBase.addIterator(job, iterator)
    }


     if(bounds != null && from != null) {
    val arange = 
    	  new ARange(rowId(layerId, bounds.colMin, bounds.rowMin, from), rowId(layerId, bounds.colMin, bounds.rowMin, from))
	      InputFormatBase.setRanges(job, Seq(arange))
      }
//    if (!tileBoundSet) setZoomBounds(job, layerId)
    //Set the filter for layer we need
    InputFormatBase.fetchColumns(job, new JPair(new Text(layerId.name), null: Text) :: Nil)
  }
}
