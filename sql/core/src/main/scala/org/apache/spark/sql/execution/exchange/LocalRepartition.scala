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

import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CARTESIAN_PRODUCT
import org.apache.spark.sql.execution.SparkPlan

case class LocalRepartition() extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!plan.conf.localRepartitionEnabled) {
      return plan
    }

    val forceForShuffleReuse = plan.conf.localRepartitionForceForShuffleReuse
    val forceForRangePartitioning = plan.conf.localRepartitionForceForRangePartitioning

    val reusedExchangeExec = plan.find {
      case _: ReusedExchangeExec => true
      case _ => false
    }

    if (reusedExchangeExec.isDefined && !forceForShuffleReuse) {
      return plan
    }

    val maxInputPartitions = plan.conf.localRepartitionMaxInputPartitions
    val maxLocalRepartitionNum = if (plan.conf.localRepartitionMaxNum > 0) {
      plan.conf.localRepartitionMaxNum
    } else {
      Int.MaxValue
    }

    var numLocalRepartition = 0

    // CartesianProductExec will create cross product between partitions from two sides.
    // So each partition will be executed multiple times. For local repartition, it will
    // be regression case.
    val newPlan = plan.transformDownWithPruning(!_.containsPattern(CARTESIAN_PRODUCT)) {
      case p =>
        val (updatedPlan, updatedNumLocalRepartition) = replace(
          p,
          forceForRangePartitioning,
          numLocalRepartition,
          maxLocalRepartitionNum,
          maxInputPartitions)

        numLocalRepartition = updatedNumLocalRepartition
        updatedPlan
    }

    newPlan
  }

  def replace(plan: SparkPlan,
      forceForRangePartitioning: Boolean,
      accuNumLocalRepartition: Int,
      maxLocalRepartitionNum: Int,
      maxInputPartitions: Int): (SparkPlan, Int) = {
    var numLocalRepartition = accuNumLocalRepartition

    val newPlan = plan.transformUp {
      case _ @ ReusedExchangeExec(output, shuffle: ShuffleExchangeExec)
        if (!shuffle.outputPartitioning.isInstanceOf[RangePartitioning] ||
          forceForRangePartitioning) && numLocalRepartition < maxLocalRepartitionNum &&
          shuffle.child.outputPartitioning.numPartitions <= maxInputPartitions =>
        numLocalRepartition += 1
        LocalRepartitionExec(
          output = output,
          outputPartitioning = shuffle.outputPartitioning,
          child = shuffle.child,
          shuffleOrigin = shuffle.shuffleOrigin)

      case shuffle @ ShuffleExchangeExec(upper, child, shuffleOrigin, _)
        if (!upper.isInstanceOf[RangePartitioning] || forceForRangePartitioning) &&
          numLocalRepartition < maxLocalRepartitionNum &&
          child.outputPartitioning.numPartitions <= maxInputPartitions =>
        numLocalRepartition += 1
        LocalRepartitionExec(
          output = child.output,
          outputPartitioning = upper,
          child = child,
          shuffleOrigin = shuffleOrigin)
    }

    (newPlan, numLocalRepartition)
  }
}
