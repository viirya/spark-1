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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class LocalQuerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  /**
   * Strip the [[QueryStageExec]] nodes off the [[SparkPlan]].
   */
  def stripQueryStagePlan(p: SparkPlan): SparkPlan = p match {
    case q: QueryStageExec => stripQueryStagePlan(q.plan)
    case other => other.withNewChildren(other.children.map(stripQueryStagePlan))
  }

  private def checkLocalQuery(
      df: DataFrame, number: Int = 1, isLocalQuery: Boolean = true): Unit = {
    val plan = stripQueryStagePlan(stripAQEPlan(df.queryExecution.executedPlan))
    val localTableScanExec = plan.collect { case l: LocalTableScanExec => l }

    assert(localTableScanExec.length == number)

    localTableScanExec.foreach { exec =>
      if (isLocalQuery) {
        assert(exec.localQuery.isDefined)
      } else {
        assert(exec.localQuery.isEmpty)
      }
    }
  }

  test("replace local query with local table") {
    withSQLConf(SQLConf.LOCAL_QUERY_ENABLED.key -> "true") {
      val df = Seq("2").toDF("a").filter($"a" > 1)
      checkAnswer(df, Seq(Row("2")))
      checkLocalQuery(df)
    }
  }

  test("don't replace local query if it has more than one partition") {
    withSQLConf(SQLConf.LOCAL_QUERY_ENABLED.key -> "true") {
      val df = Seq("2", "3").toDF("a").filter($"a" > 1)
      checkAnswer(df, Seq(Row("2"), Row("3")))
      checkLocalQuery(df, isLocalQuery = false)
    }
  }

  test("replace local aggregation with local table") {
    withSQLConf(SQLConf.LOCAL_QUERY_ENABLED.key -> "true") {
      val df = Seq(("2", 1)).toDF("a", "b").groupBy("a").agg(sum("b"))
      df.explain(true)
      checkAnswer(df, Seq(Row("2", 1)))
      checkLocalQuery(df)
    }
  }

  test("local query from scan") {
    withSQLConf(SQLConf.LOCAL_QUERY_ENABLED.key -> "true") {
      withTable("test_table") {
        spark.range(0, 101).repartition(1).write.saveAsTable("test_table")

        val df = spark.table("test_table").filter($"id" >= 100)
        checkAnswer(df, Seq(Row(100)))
        checkLocalQuery(df)
      }
    }
  }

  // todo: UDF test
}
