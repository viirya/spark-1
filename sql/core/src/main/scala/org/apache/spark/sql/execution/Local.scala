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

package org.apache.spark.sql.execution

import java.util.Properties

import scala.collection.mutable.{ArrayBuffer, HashSet, ListBuffer}

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase

/**
 * A rule to execute a SparkPlan locally. Under certain conditions, Spark will execute a plan
 * locally without submitting it as a job. The result of the execution is wrapped in a
 * LocalTableScanExec.
 */
object ExecuteAsLocalRelation extends Rule[SparkPlan] with Logging {
  /**
   * Check whether the given RDD is qualified for local execution.
   * An RDD is qualified for local execution if it does not have any parent RDDs with shuffle
   * dependencies.
   */
  def isQualified(rdd: RDD[_]): Boolean = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]

    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }

    parents.isEmpty && rdd.partitions.length == 1
  }

  /**
   * Execute the given RDD locally and return the result as a sequence.
   */
  def execute[T](env: SparkEnv, rdd: RDD[T]): Seq[T] = {
    val partitions = rdd.partitions

    if (partitions.isEmpty) {
      return Seq.empty
    }

    assert(partitions.length == 1, "Local execution does not support multiple partitions")

    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, 0L)

    val taskContext = new TaskContextImpl(
      0,
      0,
      0,
      0L,
      0,
      1,
      taskMemoryManager,
      new Properties,
      env.metricsSystem,
      TaskMetrics.empty)
    TaskContext.setTaskContext(taskContext)

    val results = rdd.iterator(partitions(0), taskContext)

    TaskContext.unset()

    val resultsArray = ArrayBuffer[T]()

    while (results.hasNext) {
      val n = results.next()
      // scalastyle:off println
      println(n)
      resultsArray += n
    }

    resultsArray.toSeq
  }

  /**
   * Check whether the given plan is supported by this rule. Columnar plans are not supported
   * because Spark ColumnarBatch is not serializable.
   */
  def isPlanSupported(plan: SparkPlan): Boolean = {
    plan match {
      case _: AdaptiveSparkPlanExec => false
      // TODO: Delta?
      case d: DataSourceV2ScanExecBase if !d.supportsColumnar =>
        // TODO: How to know the size of the relation?
        d.partitions.length == 1
      case fileSourceScanLike: FileSourceScanLike if !fileSourceScanLike.supportsColumnar =>
        fileSourceScanLike.relation.sizeInBytes < 1024 * 1024 * 1024 &&
          fileSourceScanLike.relation.inputFiles.length == 1
      case d: DataSourceScanExec if !d.supportsColumnar =>
        d.relation.sizeInBytes < 1024 * 1024 * 1024
      case _: ProjectExec | _: FilterExec | _: LocalLimitExec | _: UnionExec |
           _: WholeStageCodegenExec | _: InputAdapter |  _: LocalTableScanExec |
           _: HashAggregateExec if !plan.supportsColumnar =>
        plan.children.forall(isPlanSupported)
      case _ => false
    }
  }

  def isLocalPlan(plan: SparkPlan): Boolean = {
    plan match {
      case _: LocalTableScanExec => true
      case _ => plan.children.forall(isLocalPlan)
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = plan transformUp {
    case l: LocalTableScanExec => l
    case s: SparkPlan if isPlanSupported(s) && s.session != null =>
      val rdd = s.execute()

      if (isQualified(rdd)) {
        val results = execute(SparkEnv.get, rdd)
        // scalastyle:off println
        println(s"localized $s as LocalTableScanExec")
        logInfo(s"localized $s as LocalTableScanExec")
        LocalTableScanExec(s.output, results, None)
      } else {
       s
      }
    case o =>
      o
  }
}
