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

/**
 * An internal abstraction used to retrieve statistics from an RDD.
 */
private[spark] abstract class RDDStatistics {
  /**
   * Returns the number of bytes per partitions. Returned array enabling access the number of
   * bytes by partition id. This is a blocking call to retrieve the statistics.
   */
  def getBytesByPartitionId: Array[Long]
}

private[spark] object RDDStatistics {
  /**
   * Retrieves statistics back to driver for a given shuffle dependency. This will submit
   * a shuffle map stage immediately.
   *
   * For a shuffle dependency of an RDD with 0 partition, because we can't submit a shuffle
   * map stage for such RDD, this returns None.
   *
   * @param dependency The given `ShuffleDependency` used to submit a shuffle map stage.
   */
  def getRDDStatistics(sparkContext: SparkContext,
      dependency: ShuffleDependency[_, _, _]): Option[RDDStatistics] = {
    if (dependency.rdd.partitions.length != 0) {
      Some(new RDDStatisticsForShuffle(sparkContext, dependency))
    } else {
      // submitMapStage does not accept RDD with 0 partition.
      // So, we will not submit this dependency.
      None
    }
  }

  /**
   * Retrieves statistics back to driver for an RDD. This will submit a shuffle map stage
   * immediately.
   *
   * Because we can't submit a shuffle map stage for an RDD with 0 partition, this returns
   * None for such input RDD.
   *
   * @param rdd        The RDD object to retrieve data statistics from.
   * @param serializer The serializer to use, to use. If not set explicitly then the default
   *                   serializer, as specified by `spark.serializer` config option, will be used.
   */
  def getRDDStatistics[T: ClassTag](sparkContext: SparkContext,
      rdd: RDD[T], serializer: Serializer = SparkEnv.get.serializer): Option[RDDStatistics] = {
    if (rdd.getNumPartitions != 0) {
      Some(new RDDDataStatistics[T](sparkContext, rdd, serializer))
    } else {
      // submitMapStage does not accept RDD with 0 partition.
      // So, we will not submit this dependency.
      None
    }
  }
}

/**
 * Retrieves statistics back to driver for an RDD. Specifically, this performs a shuffle map stage
 * submission via `DAGScheduler` (see `DAGScheduler.submitMapStage` API) and collects statistics
 * about the outputs of the given RDD. This will submit a shuffle map stage immediately when
 * initializing this object.
 *
 * Notice: In order to collect statistics from given RDD, this submits a shuffle map stage that
 * won't change the data partitions of the given RDD, but only write down data into disks with
 * its original partition index.
 *
 * Notice: For some RDDs from Spark SQL, it is responsible for the caller to do copying of input
 * rows if needed.
 *
 * @param rdd        The RDD object to retrieve data statistics from.
 * @param serializer The serializer to use, to use.
 */
private[spark] class RDDDataStatistics[T: ClassTag] private[rdd](val sparkContext: SparkContext,
    val rdd: RDD[T], val serializer: Serializer) extends RDDStatistics {

  /**
   * Makes an RDD with tuples of each input record with corresponding partition id.
   * TODO: Use `MutablePair` if we know we don't need to copy object during shuffle. Please
   * see `ShuffleExchangeExec.needToCopyObjectsBeforeShuffle`.
   */
  private val rddWithPartitionIds = rdd.mapPartitionsWithIndexInternal((_, iter) => {
    val partitionId = TaskContext.get().partitionId()
    iter.map { record => (partitionId, record) }
  })

  private val dependency =
    new ShuffleDependency[Int, T, T](
      rddWithPartitionIds,
      new PartitionIdPassthrough(rdd.getNumPartitions),
      serializer)

  private val submittedStageFuture = sparkContext.submitMapStage(dependency)

  private lazy val mapOutputStatistics: MapOutputStatistics =
    submittedStageFuture.get()

  override def getBytesByPartitionId: Array[Long] =
    mapOutputStatistics.bytesByPartitionId

  def getShuffleDependency: ShuffleDependency[Int, T, T] = dependency
}

/**
 * Retrieves statistics back to driver for a given shuffle dependency. This will submit
 * a shuffle map stage immediately when initializing this object.
 *
 * @param dependency The given `ShuffleDependency` used to submit a shuffle map stage.
 */
private[spark] class RDDStatisticsForShuffle private[rdd](val sparkContext: SparkContext,
    val dependency: ShuffleDependency[_, _, _]) extends RDDStatistics {

  private val submittedStageFuture = sparkContext.submitMapStage(dependency)

  private lazy val mapOutputStatistics: MapOutputStatistics =
    submittedStageFuture.get()

  override def getBytesByPartitionId: Array[Long] =
    mapOutputStatistics.bytesByPartitionId
}
