package geotrellis.spark.io

import geotrellis.spark.KeyBounds


trait Writer[K, J, V] extends ((K,V) => Unit) {
  def write(key: K, value: V, kb: Option[KeyBounds[J]]): Unit
  def write(key: K, value: V): Unit = write(key, value, None)
  def apply(key: K, value: V): Unit = write(key, value, None)
}
