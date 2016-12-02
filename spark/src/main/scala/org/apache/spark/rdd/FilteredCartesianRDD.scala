/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.Utils


sealed class FilteredCartesianRDD[T: ClassTag, U: ClassTag, V: ClassTag](
  sc: SparkContext,
  pred: (T, U) => Boolean,
  metapred: (V, V) => Boolean,
  var rdd1 : RDD[T], metardd1 : RDD[V],
  var rdd2 : RDD[U], metardd2 : RDD[V]
) extends RDD[(T, U)](sc, Nil) with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    val part1 = currSplit.s1 // partition for (|meta)rdd1
    val part2 = currSplit.s2 // partition for (|meta)rdd2
    val meta1 = metardd1.iterator(part1, context).next
    val meta2 = metardd2.iterator(part2, context).next

    if (metapred(meta1, meta2)) {
      for (x <- rdd1.iterator(part1, context);
           y <- rdd2.iterator(part2, context);
           if (pred(x,y))) yield (x, y)
    }
    else Iterator.empty
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
