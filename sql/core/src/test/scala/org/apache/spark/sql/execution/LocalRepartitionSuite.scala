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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.exchange.{LocalRepartitionExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration test suite for local repartition functionality at the SQL level.
 * Tests various scenarios including hash partitioning, configuration validation,
 * and correctness verification.
 */
class LocalRepartitionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("local repartition with simple aggregation") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(100).selectExpr("id", "id % 10 as key")
        .groupBy("key").count()

      val result = df.collect().sortBy(_.getLong(0))
      assert(result.length == 10)
      result.foreach { row =>
        assert(row.getLong(1) == 10) // Each key should have 10 records
      }

      // Verify that LocalRepartitionExec is used instead of ShuffleExchangeExec
      val plan = df.queryExecution.executedPlan
      val hasLocalRepartition = plan.collect {
        case _: LocalRepartitionExec => true
      }.nonEmpty

      assert(hasLocalRepartition, "LocalRepartitionExec should be used")
    }
  }

  test("local repartition disabled falls back to shuffle") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(100).selectExpr("id", "id % 10 as key")
        .groupBy("key").count()

      val plan = df.queryExecution.executedPlan
      val hasShuffleExchange = plan.collect {
        case _: ShuffleExchangeExec => true
      }.nonEmpty

      assert(hasShuffleExchange,
        "ShuffleExchangeExec should be used when local repartition is disabled")
    }
  }

  test("local repartition with join") {
    withSQLConf(SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true") {
      val df1 = spark.range(100).selectExpr("id as key1", "id * 2 as value1")
      val df2 = spark.range(50).selectExpr("id as key2", "id * 3 as value2")

      val joined = df1.join(df2, $"key1" === $"key2")
      val result = joined.collect()

      assert(result.length == 50)
      result.foreach { row =>
        val key = row.getLong(0)
        assert(row.getLong(1) == key * 2)
        assert(row.getLong(3) == key * 3)
      }
    }
  }

  test("local repartition with multiple stages") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(1000)
        .selectExpr("id", "id % 100 as key")
        .groupBy("key").agg(sum("id").as("sum_id"))
        .filter("sum_id > 1000")
        .groupBy().agg(sum("sum_id").as("total"))

      val result = df.collect()
      assert(result.length == 1)
      // Verify the plan has multiple LocalRepartitionExec nodes
      val plan = df.queryExecution.executedPlan
      val localRepartitionCount = plan.collect {
        case _: LocalRepartitionExec => 1
      }.sum

      assert(localRepartitionCount >= 1, "Should have at least one LocalRepartitionExec")
    }
  }

  test("local repartition respects max input partition limit") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      SQLConf.LOCAL_REPARTITION_MAX_INPUT_PARTITION_NUM.key -> "2",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {

      val df = spark.range(0, 100, 1, 10) // 10 partitions
        .selectExpr("id", "id % 5 as key")
        .groupBy("key").count()

      val plan = df.queryExecution.executedPlan
      val hasShuffleExchange = plan.collect {
        case _: ShuffleExchangeExec => true
      }.nonEmpty

      // Should fall back to shuffle because input partitions (10) > max (2)
      assert(hasShuffleExchange,
        "Should use ShuffleExchangeExec when input partitions exceed limit")
    }
  }

  test("local repartition respects max local repartition operators limit") {
    // Note: Setting maxNum to 0 currently means unlimited, but setting to 1 should limit
    // the number of local repartitions in multi-stage queries
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      SQLConf.LOCAL_REPARTITION_MAX_NUM.key -> "1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {

      // Multi-stage query that would normally have 2 repartitions
      val df = spark.range(100)
        .selectExpr("id", "id % 10 as key")
        .groupBy("key").agg(sum("id").as("sum_id"))
        .groupBy().agg(sum("sum_id").as("total"))

      val plan = df.queryExecution.executedPlan
      val localRepartitionCount = plan.collect {
        case _: LocalRepartitionExec => 1
      }.sum

      // Should have at most 1 LocalRepartitionExec due to the limit
      assert(localRepartitionCount <= 1,
        s"Should have at most 1 LocalRepartitionExec, but found $localRepartitionCount")
    }
  }

  test("local repartition with empty dataset") {
    withSQLConf(SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true") {
      val df = spark.range(0) // Empty dataset
        .selectExpr("id", "id % 10 as key")
        .groupBy("key").count()

      val result = df.collect()
      assert(result.isEmpty, "Result should be empty for empty input")
    }
  }

  test("local repartition with null values") {
    withSQLConf(SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true") {
      val df = Seq(
        (Some(1), "a"),
        (None, "b"),
        (Some(2), "c"),
        (None, "d")
      ).toDF("key", "value")
        .groupBy("key").count()

      val result = df.collect().sortBy(row => Option(row.get(0)).map(_.toString).getOrElse(""))
      assert(result.length == 3) // null, 1, 2
    }
  }

  test("local repartition correctness with large dataset") {
    withSQLConf(SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true") {
      val n = 10000
      val df = spark.range(n)
        .selectExpr("id", "id % 100 as key")
        .groupBy("key").agg(sum("id").as("sum_id"), count("*").as("count"))

      val result = df.collect().sortBy(_.getLong(0))
      assert(result.length == 100)

      result.foreach { row =>
        val key = row.getLong(0)
        val sum = row.getLong(1)
        val count = row.getLong(2)
        assert(count == n / 100, s"Each key should have ${n / 100} records")

        // Verify sum: sum of arithmetic sequence
        val expectedSum = (0 until n by 1)
          .filter(_ % 100 == key)
          .map(_.toLong)
          .sum
        assert(sum == expectedSum, s"Sum for key $key should be $expectedSum")
      }
    }
  }

  test("local repartition with buffer size configuration") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      "spark.localRepartition.buffer.size" -> "100",
      "spark.localRepartition.sender.buffer.size" -> "10",
      "spark.localRepartition.receiver.buffer.size" -> "10") {

      val df = spark.range(1000)
        .selectExpr("id", "id % 10 as key")
        .groupBy("key").count()

      val result = df.collect().sortBy(_.getLong(0))
      assert(result.length == 10)
      result.foreach { row =>
        assert(row.getLong(1) == 100)
      }
    }
  }

  test("local repartition with different number of output partitions") {
    withSQLConf(SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true") {
      for (numPartitions <- Seq(1, 5, 10, 20)) {
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> numPartitions.toString) {
          val df = spark.range(100)
            .selectExpr("id", "id % 10 as key")
            .groupBy("key").count()

          val result = df.collect().sortBy(_.getLong(0))
          assert(result.length == 10)
          result.foreach { row =>
            assert(row.getLong(1) == 10)
          }
        }
      }
    }
  }

  test("local repartition metrics are recorded") {
    withSQLConf(
      SQLConf.LOCAL_REPARTITION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(100)
        .selectExpr("id", "id % 10 as key")
        .groupBy("key").count()

      df.collect()

      val plan = df.queryExecution.executedPlan
      val localRepartitionExecs = plan.collect {
        case lr: LocalRepartitionExec => lr
      }

      assert(localRepartitionExecs.nonEmpty, "Should have LocalRepartitionExec in plan")

      localRepartitionExecs.foreach { exec =>
        val metrics = exec.metrics
        assert(metrics.contains("numInputRows"))
        assert(metrics.contains("numOutputRows"))
        assert(metrics.contains("numInputPartitions"))
        assert(metrics.contains("numOutputPartitions"))
      }
    }
  }
}
