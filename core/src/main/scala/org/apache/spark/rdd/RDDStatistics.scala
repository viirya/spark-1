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

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.MutablePair

/**
 * An abstraction used to retrieve statistics from an RDD.
 */
abstract class RDDStatistics {
  /**
   * Returns the number of bytes per partitions. Returned array enabling access the number of
   * bytes by partition id.
   */
  def getBytesByPartitionId: Array[Long]
}

/**
 * Retrieves statistics back to driver for an RDD. Specifically, this performs a shuffle map stage
 * submission via `DAGScheduler` (see `DAGScheduler.submitMapStage` API) and collects statistics
 * about the outputs of the given RDD.
 *
 * Notice: This won't change the data partitions of the given RDD.
 *
 * Notice: For some RDDs from Spark SQL, it is responsible for the caller to do copying of input
 * rows if needed.
 *
 * @param rdd        The RDD object to retrieve data statistics from.
 * @param serializer The serializer to use, to use. If not set explicitly then the default
 *                   serializer,
 *                   as specified by `spark.serializer` config option, will be used.
 */
class RDDDataStatistics[T: ClassTag](val sparkContext: SparkContext,
    val rdd: RDD[T], val serializer: Serializer = SparkEnv.get.serializer) extends RDDStatistics {

  /**
   * Makes an RDD with tuples of each input record with corresponding partition id.
   */
  private val rddWithPartitionIds = rdd.mapPartitionsWithIndexInternal((_, iter) => {
    val partitionId = TaskContext.get().partitionId()
    val mutablePair = new MutablePair[Int, T]()
    iter.map { record => mutablePair.update(partitionId, record) }
  })

  private lazy val dependency =
    new ShuffleDependency[Int, T, T](
      rddWithPartitionIds,
      new PartitionIdPassthrough(rdd.getNumPartitions),
      serializer)

  private lazy val mapOutputStatistics: MapOutputStatistics = {
    val submittedStageFuture = sparkContext.submitMapStage(dependency)
    submittedStageFuture.get()
  }

  def getBytesByPartitionId: Array[Long] = mapOutputStatistics.bytesByPartitionId

  def getShuffleDependency: ShuffleDependency[Int, T, T] = dependency
}

/**
 * Retrieves statistics back to driver for a given shuffle dependency.
 *
 * @param dependency The given `ShuffleDependency` used to submit a shuffle map stage.
 */
class RDDStatisticsForShuffle(val sparkContext: SparkContext,
    val dependency: ShuffleDependency[_, _, _]) {

  private lazy val mapOutputStatistics: MapOutputStatistics = {
    val submittedStageFuture = sparkContext.submitMapStage(dependency)
    submittedStageFuture.get()
  }

  def getBytesByPartitionId: Array[Long] = mapOutputStatistics.bytesByPartitionId
}
