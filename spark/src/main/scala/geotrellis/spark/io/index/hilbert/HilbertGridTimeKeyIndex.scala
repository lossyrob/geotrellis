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

object HilbertGridTimeKeyIndex {
  def apply(minKey: GridTimeKey, maxKey: GridTimeKey, spatialResolution: Int, timeResolution: Int): HilbertGridTimeKeyIndex =
    apply(KeyBounds(minKey, maxKey), spatialResolution, timeResolution)

  def apply(keyBounds: KeyBounds[GridTimeKey], spatialResolution: Int, timeResolution: Int): HilbertGridTimeKeyIndex =
    apply(keyBounds, spatialResolution, spatialResolution, timeResolution)

  def apply(keyBounds: KeyBounds[GridTimeKey], xResolution: Int, yResolution: Int, timeResolution: Int): HilbertGridTimeKeyIndex =
    new HilbertGridTimeKeyIndex(keyBounds, xResolution, yResolution, timeResolution)
}

class HilbertGridTimeKeyIndex(
  val keyBounds: KeyBounds[GridTimeKey],
  val xResolution: Int,
  val yResolution: Int,
  val timeResolution: Int
) extends KeyIndex[GridTimeKey] {
  val startMillis = keyBounds.minKey.timeKey.time.getMillis
  val timeWidth = keyBounds.maxKey.timeKey.time.getMillis - startMillis
  val timeBinCount = math.pow(2, timeResolution)
  val minKey = keyBounds.minKey.spatialKey

  @transient lazy val chc = {
    val dimensionSpec =
      new MultiDimensionalSpec(
        List(
          xResolution,
          yResolution,
          timeResolution
        ).map(new java.lang.Integer(_))
      )

    new CompactHilbertCurve(dimensionSpec)
  }

  def binTime(key: GridTimeKey): Long = {
    // index requires right bound to be exclusive but KeyBounds do not, fake that.
    val bin = (((key.timeKey.time.getMillis - startMillis) * timeBinCount) / timeWidth)
    (if (bin == timeBinCount) bin - 1  else bin).toLong
  }

  def toIndex(key: GridTimeKey): Long = {
    val bitVectors =
      Array(
        BitVectorFactories.OPTIMAL.apply(xResolution),
        BitVectorFactories.OPTIMAL.apply(yResolution),
        BitVectorFactories.OPTIMAL.apply(timeResolution)
      )

    val col = key.spatialKey.col - minKey.col
    val row = key.spatialKey.row - minKey.row
    bitVectors(0).copyFrom(col.toLong)
    bitVectors(1).copyFrom(row.toLong)
    bitVectors(2).copyFrom(binTime(key))

    val hilbertBitVector = BitVectorFactories.OPTIMAL.apply(chc.getSpec.sumBitsPerDimension)

    chc.index(bitVectors, 0, hilbertBitVector)

    hilbertBitVector.toExactLong
  }

  // Note: this function will happily index outside of the index keyBounds
  def indexRanges(keyRange: (GridTimeKey, GridTimeKey)): Seq[(Long, Long)] = {
    val ranges: java.util.List[LongRange] =
      List( //LongRange is exclusive on upper bound, adjusting for it here with + 1
        LongRange.of(keyRange._1.spatialKey.col - minKey.col, keyRange._2.spatialKey.col - minKey.col + 1),
        LongRange.of(keyRange._1.spatialKey.row - minKey.row, keyRange._2.spatialKey.row - minKey.row + 1),
        LongRange.of(binTime(keyRange._1), binTime(keyRange._2) + 1)
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
      new PlainFilterCombiner[LongRange, java.lang.Long, LongContent, LongRange](LongRange.of(0, 1))

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
