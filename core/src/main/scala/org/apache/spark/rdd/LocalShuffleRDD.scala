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

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, LocalRepartitionDependency, Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi

class LocalShufflePartition(rddId: Int, val index: Int) extends Partition {
  override def hashCode(): Int = 31 * (31 + rddId) + index
  override def equals(other: Any): Boolean = super.equals(other)
}

@DeveloperApi
class LocalShuffleRDD[T: ClassTag](
    sc: SparkContext,
    var rdd: RDD[T],
    part: Partitioner)
  extends RDD[T](sc, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new LocalRepartitionDependency(rdd))
  }

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  override protected def getPartitions: Array[Partition] = {
    val result = new Array[Partition](part.numPartitions)

    for (i <- 0 until part.numPartitions) {
      result(i) = new LocalShufflePartition(id, i)
    }

    result
  }
}
