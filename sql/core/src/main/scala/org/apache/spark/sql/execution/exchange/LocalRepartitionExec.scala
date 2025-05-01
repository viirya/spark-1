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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.{Partitioner, ShuffleDependency, SparkEnv}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.MutablePair

case class LocalRepartitionExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow],
    shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends Exchange {

  override def supportsColumnar: Boolean = false

  override val metrics = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()

    val part = ShuffleExchangeExec.getPartitioner(
      childRDD,
      child.output,
      outputPartitioning)

    val inputRDD: RDD[Product2[Int, InternalRow]] =
      childRDD.mapPartitionsWithIndexInternal((_, iter) => {
        val getPartitionKey =
          ShuffleExchangeExec.getPartitionKeyExtractor(
            child.output, outputPartitioning)
        iter.map { row => // we need to copy the row because local repartition buffers the rows
          metrics("numInputRows") += 1
          val mutablePair = new MutablePair[Int, InternalRow]()
          mutablePair.update(part.getPartition(getPartitionKey(row)), row.copy()) }
      })

    val partitioner = new SQLMutablePairPartitioner(part.numPartitions)

    // Serialize the tasks
    // HACK
    inputRDD.isBarrier()
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    val serializedRDD = JavaUtils.bufferToArray(
      closureSerializer.serialize(inputRDD: AnyRef))

    inputRDD.localRepartition(partitioner, serializedRDD).map { pair =>
      metrics("numOutputRows") += 1
      pair._2
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

class SQLMutablePairPartitioner(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Product2[Int, InternalRow]]._1
}
