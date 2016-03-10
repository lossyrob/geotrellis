/*
 * Copyright (c) 2014 DigitalGlobe.
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

package geotrellis.spark.io.kryo

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }

import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}

import com.esotericsoftware.kryo.Kryo
import de.javakaffee.kryoserializers._

import java.util.{Arrays, Collections}

class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[(_,_)])
    kryo.register(classOf[::[_]])
    kryo.register(classOf[geotrellis.raster.BitArrayTile])
    kryo.register(classOf[geotrellis.raster.ByteArrayFiller])
    kryo.register(classOf[geotrellis.raster.FloatArrayTile])
    kryo.register(classOf[geotrellis.raster.DoubleArrayTile])
    kryo.register(classOf[geotrellis.raster.ShortArrayTile])
    kryo.register(classOf[geotrellis.raster.IntArrayTile])
    kryo.register(classOf[geotrellis.spark.GridKey])
    kryo.register(classOf[geotrellis.spark.GridTimeKey])
    kryo.register(classOf[org.joda.time.DateTime], new jodatime.JodaDateTimeSerializer)
    kryo.register(classOf[org.joda.time.Interval], new jodatime.JodaIntervalSerializer)
    kryo.register(classOf[geotrellis.spark.io.index.rowmajor.RowMajorGridKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.zcurve.ZGridKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.zcurve.ZGridTimeKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.hilbert.HilbertGridKeyIndex])
    kryo.register(classOf[geotrellis.spark.io.index.hilbert.HilbertGridTimeKeyIndex])
    kryo.register(classOf[geotrellis.vector.ProjectedExtent])
    kryo.register(classOf[geotrellis.vector.Extent])
    kryo.register(classOf[geotrellis.proj4.CRS])

    // UnmodifiableCollectionsSerializer.registerSerializers(kryo)
    kryo.register( Arrays.asList( "" ).getClass, new ArraysAsListSerializer )
    kryo.register( Collections.EMPTY_LIST.getClass, new CollectionsEmptyListSerializer() )
    kryo.register( Collections.EMPTY_MAP.getClass, new CollectionsEmptyMapSerializer() )
    kryo.register( Collections.EMPTY_SET.getClass, new CollectionsEmptySetSerializer() )
    kryo.register( Collections.singletonList( "" ).getClass, new CollectionsSingletonListSerializer )
    kryo.register( Collections.singleton( "" ).getClass, new CollectionsSingletonSetSerializer )
    kryo.register( Collections.singletonMap( "", "" ).getClass, new CollectionsSingletonMapSerializer )
    kryo.register(geotrellis.spark.buffer.BufferTiles.Center.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.Top.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.Bottom.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.Left.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.Right.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.TopLeft.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.TopRight.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.BottomLeft.getClass)
    kryo.register(geotrellis.spark.buffer.BufferTiles.BottomRight.getClass)

    /* Exhaustive Registration */
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Coordinate]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.LinearRing]])
    kryo.register(classOf[Array[com.vividsolutions.jts.geom.Polygon]])
    kryo.register(classOf[Array[geotrellis.raster.Tile]])
    kryo.register(classOf[Array[geotrellis.spark.io.avro.AvroRecordCodec[Any]]])
    kryo.register(classOf[Array[geotrellis.spark.GridTimeKey]])
    kryo.register(classOf[Array[geotrellis.spark.GridKey]])
    kryo.register(classOf[Array[geotrellis.vector.Feature[Any,Any]]])
    kryo.register(classOf[Array[geotrellis.vector.MultiPolygon]])
    kryo.register(classOf[Array[geotrellis.vector.Point]])
    kryo.register(classOf[Array[geotrellis.vector.Polygon]])
    kryo.register(classOf[Array[scala.collection.Seq[Any]]])
    kryo.register(classOf[Array[scala.Tuple2[Any, Any]]])
    kryo.register(classOf[Array[scala.Tuple3[Any, Any, Any]]])
    kryo.register(classOf[com.vividsolutions.jts.geom.Coordinate])
    kryo.register(classOf[com.vividsolutions.jts.geom.Envelope])
    kryo.register(classOf[com.vividsolutions.jts.geom.GeometryFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequence])
    kryo.register(classOf[com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory])
    kryo.register(classOf[com.vividsolutions.jts.geom.LinearRing])
    kryo.register(classOf[com.vividsolutions.jts.geom.MultiPolygon])
    kryo.register(classOf[com.vividsolutions.jts.geom.Point])
    kryo.register(classOf[com.vividsolutions.jts.geom.Polygon])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel])
    kryo.register(classOf[com.vividsolutions.jts.geom.PrecisionModel.Type])
    kryo.register(classOf[geotrellis.raster.ArrayMultibandTile])
    kryo.register(classOf[geotrellis.raster.ArrayTile])
    kryo.register(classOf[geotrellis.raster.ByteArrayTile])
    kryo.register(classOf[geotrellis.raster.CellGrid])
    kryo.register(classOf[geotrellis.raster.CellSize])
    kryo.register(classOf[geotrellis.raster.CompositeTile])
    kryo.register(classOf[geotrellis.raster.ConstantTile])
    kryo.register(classOf[geotrellis.raster.CroppedTile])
    kryo.register(classOf[geotrellis.raster.DoubleConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.EmptyTile])
    kryo.register(classOf[geotrellis.raster.FloatConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.FloatUserDefinedNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.FloatUserDefinedNoDataCellType])
    kryo.register(classOf[geotrellis.raster.GridBounds])
    kryo.register(classOf[geotrellis.raster.GridExtent])
    kryo.register(classOf[geotrellis.raster.histogram.ArrayHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.CompressedArrayHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.ConstantHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.FastMapHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.Histogram[Any]])
    kryo.register(classOf[geotrellis.raster.histogram.MapHistogram])
    kryo.register(classOf[geotrellis.raster.histogram.MutableHistogram[Any]])
    kryo.register(classOf[geotrellis.raster.histogram.StreamingHistogram])
    kryo.register(classOf[geotrellis.raster.IntBasedArrayTile])
    kryo.register(classOf[geotrellis.raster.IntConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.IterableTile])
    kryo.register(classOf[geotrellis.raster.KernelStamper])
    kryo.register(classOf[geotrellis.raster.LazyConvertedArrayTile])
    kryo.register(classOf[geotrellis.raster.MultibandTile])
    kryo.register(classOf[geotrellis.raster.MutableArrayTile])
    kryo.register(classOf[geotrellis.raster.summary.polygonal.MeanResult])
    kryo.register(classOf[geotrellis.raster.ProjectedRaster[Any]])
    kryo.register(classOf[geotrellis.raster.Raster[Any]])
    kryo.register(classOf[geotrellis.raster.RasterExtent])
    kryo.register(classOf[geotrellis.raster.ShortConstantNoDataArrayTile])
    kryo.register(classOf[geotrellis.raster.Tile])
    kryo.register(classOf[geotrellis.raster.TileLayout])
    kryo.register(geotrellis.raster.BitCellType.getClass)
    kryo.register(geotrellis.raster.ByteCellType.getClass)
    kryo.register(geotrellis.raster.UByteCellType.getClass)
    kryo.register(geotrellis.raster.ShortCellType.getClass)
    kryo.register(geotrellis.raster.UShortCellType.getClass)
    kryo.register(geotrellis.raster.IntCellType.getClass)
    kryo.register(geotrellis.raster.FloatCellType.getClass)
    kryo.register(geotrellis.raster.DoubleCellType.getClass)
    kryo.register(geotrellis.raster.ByteConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.UByteConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.ShortConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.UShortConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.IntConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.FloatConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.DoubleConstantNoDataCellType.getClass)
    kryo.register(geotrellis.raster.ByteUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.UByteUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.ShortUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.UShortUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.IntUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.FloatUserDefinedNoDataCellType.getClass)
    kryo.register(geotrellis.raster.DoubleUserDefinedNoDataCellType.getClass)
    kryo.register(classOf[geotrellis.raster.UByteArrayTile])
    kryo.register(classOf[geotrellis.raster.UByteRawArrayTile])
    kryo.register(classOf[geotrellis.raster.UShortArrayTile])
    kryo.register(classOf[geotrellis.spark.buffer.BufferSizes])
    kryo.register(classOf[geotrellis.spark.io.avro.AvroRecordCodec[Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.AvroUnionCodec[Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.codecs.KeyValueRecordCodec[Any,Any]])
    kryo.register(classOf[geotrellis.spark.io.avro.codecs.TupleCodec[Any,Any]])
    kryo.register(classOf[geotrellis.spark.KeyBounds[Any]])
    kryo.register(classOf[geotrellis.vector.Feature[Any,Any]])
    kryo.register(classOf[geotrellis.vector.Geometry])
    kryo.register(classOf[geotrellis.vector.GeometryCollection])
    kryo.register(classOf[geotrellis.vector.Line])
    kryo.register(classOf[geotrellis.vector.MultiGeometry])
    kryo.register(classOf[geotrellis.vector.MultiLine])
    kryo.register(classOf[geotrellis.vector.MultiPoint])
    kryo.register(classOf[geotrellis.vector.MultiPolygon])
    kryo.register(classOf[geotrellis.vector.Point])
    kryo.register(classOf[geotrellis.vector.Polygon])
    kryo.register(classOf[geotrellis.vector.SpatialIndex[Any]])
    kryo.register(classOf[java.lang.Class[Any]])
    kryo.register(classOf[java.util.HashMap[Any,Any]])
    kryo.register(classOf[java.util.HashSet[Any]])
    kryo.register(classOf[java.util.LinkedHashMap[Any,Any]])
    kryo.register(classOf[java.util.LinkedHashSet[Any]])
    kryo.register(classOf[org.apache.hadoop.io.BytesWritable])
    kryo.register(classOf[org.apache.hadoop.io.LongWritable])
    kryo.register(classOf[Array[org.apache.hadoop.io.LongWritable]])
    kryo.register(classOf[org.codehaus.jackson.node.BooleanNode])
    kryo.register(classOf[org.codehaus.jackson.node.IntNode])
    kryo.register(classOf[org.osgeo.proj4j.CoordinateReferenceSystem])
    kryo.register(classOf[org.osgeo.proj4j.datum.Datum])
    kryo.register(classOf[org.osgeo.proj4j.datum.Ellipsoid])
    kryo.register(classOf[org.osgeo.proj4j.proj.LambertConformalConicProjection])
    kryo.register(classOf[org.osgeo.proj4j.units.Unit])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofInt])
    kryo.register(classOf[scala.collection.mutable.WrappedArray$ofRef])
    kryo.register(classOf[scala.collection.Seq[Any]])
    kryo.register(classOf[scala.Tuple3[Any, Any, Any]])
    kryo.register(geotrellis.proj4.LatLng.getClass)
    kryo.register(geotrellis.spark.EmptyBounds.getClass)
    kryo.register(scala.collection.immutable.Nil.getClass)
    kryo.register(scala.math.Ordering.Double.getClass)
    kryo.register(scala.math.Ordering.Float.getClass)
    kryo.register(scala.math.Ordering.Int.getClass)
    kryo.register(scala.math.Ordering.Long.getClass)
    kryo.register(scala.None.getClass)

    /* Special Handling: Avro */
    kryo.register(new Field("a", Schema.create(Type.NULL), null, null: Object).order.getClass)
    classOf[org.apache.avro.Schema]
      .getDeclaredClasses
      .foreach({ c => kryo.register(c) })

    UnmodifiableCollectionsSerializer.registerSerializers( kryo )
    SynchronizedCollectionsSerializer.registerSerializers( kryo )
  }
}
