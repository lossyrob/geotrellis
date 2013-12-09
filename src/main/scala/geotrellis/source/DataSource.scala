package geotrellis.source

import scala.collection._
import scala.collection.generic._
import geotrellis.raster.op._
import geotrellis._
import geotrellis.statistics._

/**
 * Represents a data source that may be distributed across machines (logical data source) 
 * or loaded in memory on a specific machine. 
  */
trait DataSource[+T,+V] extends DataSourceLike[T,V,DataSource[T,V]] {
}

object DataSource {
  def convergeSeq[A](elementOps:(Op[Seq[Op[A]]])) = {
    logic.Collect(elementOps)
  }

  def fromValues[T](elements:T*):DataSource[T,Seq[T]] =
    fromValues(elements)

  def fromValues[T](elements:Seq[T])(implicit d:DI):DataSource[T,Seq[T]] =
    apply(Literal(elements.map(Literal(_))))

  def apply[T](elements:Op[Seq[Op[T]]]):DataSource[T,Seq[T]] = {
    val builder:DataSourceBuilder[T,Seq[T]] = new DataSourceBuilder(convergeSeq)
    builder.setOp(elements)
    builder.result
  }
}
