package geotrellis.spark.io.index.hilbert

import geotrellis.spark._
import geotrellis.spark.io.index.KeyIndex

import com.google.uzaygezen.core.CompactHilbertCurve
import com.google.uzaygezen.core.MultiDimensionalSpec
import com.google.uzaygezen.core.BitVectorFactories
import com.google.uzaygezen.core.BacktrackingQueryBuilder
import com.google.uzaygezen.core.RegionInspector
import com.google.uzaygezen.core.SimpleRegionInspector
import com.google.uzaygezen.core.LongContent
import com.google.uzaygezen.core.PlainFilterCombiner
import com.google.uzaygezen.core.ZoomingSpaceVisitorAdapter
import com.google.uzaygezen.core.ranges.LongRange
import com.google.uzaygezen.core.ranges.LongRangeHome

import com.google.common.base.Functions

import scala.collection.JavaConversions._
import spire.syntax.cfor._

object HilbertAltSpatialKeyIndex {
  def apply(minKey: AltSpatialKey, maxKey: AltSpatialKey, spatialResolution: Int, zResolution: Int): HilbertAltSpatialKeyIndex =
    apply(KeyBounds(minKey, maxKey), spatialResolution, zResolution)

  def apply(keyBounds: KeyBounds[AltSpatialKey], spatialResolution: Int, zResolution: Int): HilbertAltSpatialKeyIndex =
    apply(keyBounds, spatialResolution, spatialResolution, zResolution)

  def apply(keyBounds: KeyBounds[AltSpatialKey], xResolution: Int, yResolution: Int, zResolution: Int): HilbertAltSpatialKeyIndex =
    new HilbertAltSpatialKeyIndex(keyBounds, xResolution, yResolution, zResolution)
}

class HilbertAltSpatialKeyIndex(
  keyBounds: KeyBounds[AltSpatialKey],
  xResolution: Int,
  yResolution: Int,
  zResolution: Int
) extends KeyIndex[AltSpatialKey] {
  val minZ = keyBounds.minKey.z
  val zWidth = keyBounds.maxKey.z - minZ
  val zBinCount = math.pow(2, zResolution)

  @transient lazy val chc = {
    val dimensionSpec =
      new MultiDimensionalSpec(
        List(
          math.pow(2, xResolution).toInt,
          math.pow(2, yResolution).toInt,
          math.pow(2, zResolution).toInt
        ).map(new java.lang.Integer(_))
      )

    new CompactHilbertCurve(dimensionSpec)
  }

  def binZ(key: AltSpatialKey): Long = {
    // index requires right bound to be exclusive but KeyBounds do not, fake that.
    val bin = (((key.z - minZ) * zBinCount) / zWidth)
    (if (bin == zBinCount) bin - 1  else bin).toLong
  }

  def toIndex(key: AltSpatialKey): Long = {
    val bitVectors =
      Array(
        BitVectorFactories.OPTIMAL.apply(xResolution),
        BitVectorFactories.OPTIMAL.apply(yResolution),
        BitVectorFactories.OPTIMAL.apply(zResolution)
      )

    bitVectors(0).copyFrom(key.spatialKey.col.toLong)
    bitVectors(1).copyFrom(key.spatialKey.row.toLong)
    bitVectors(2).copyFrom(binZ(key))

    val hilbertBitVector =
      BitVectorFactories.OPTIMAL.apply(chc.getSpec.sumBitsPerDimension)

    chc.index(bitVectors, 0, hilbertBitVector)

    hilbertBitVector.toExactLong
  }

  // Note: this function will happily index outside of the index keyBounds
  def indexRanges(keyRange: (AltSpatialKey, AltSpatialKey)): Seq[(Long, Long)] = {
    val ranges: java.util.List[LongRange] =
      List( //LongRange is exclusive on upper bound, adjusting for it here with + 1
        LongRange.of(keyRange._1.spatialKey.col, keyRange._2.spatialKey.col + 1),
        LongRange.of(keyRange._1.spatialKey.row, keyRange._2.spatialKey.row + 1),
        LongRange.of(binZ(keyRange._1), binZ(keyRange._2) + 1)
      )

    val  regionInspector: RegionInspector[LongRange, LongContent] =
      SimpleRegionInspector.create(
        List(ranges),
        new LongContent(1),
        Functions.identity[LongRange](),
        LongRangeHome.INSTANCE,
        new LongContent(0L)
      )

    val combiner =
      new PlainFilterCombiner[LongRange, java.lang.Long, LongContent, LongRange](LongRange.of(0, 1));

    val queryBuilder =
      BacktrackingQueryBuilder.create(
        regionInspector,
        combiner,
        Int.MaxValue,
        true,
        LongRangeHome.INSTANCE,
        new LongContent(0L)
      )

    chc.accept(new ZoomingSpaceVisitorAdapter(chc, queryBuilder))
    val filteredIndexRanges = queryBuilder.get.getFilteredIndexRanges
    val size = filteredIndexRanges.size
    val result = Array.ofDim[(Long, Long)](size)

    cfor(0)(_ < size, _ + 1) { i =>
      val range = filteredIndexRanges.get(i)
      // uzaygezen ranges are exclusive on the interval, GeoTrellis index ranges are inclusive, adjusting here.
      result(i) = (range.getIndexRange.getStart, range.getIndexRange.getEnd - 1)
    }

    result
  }
}
