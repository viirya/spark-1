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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LocalRelation, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.types.StringType

class ResolveEncodersInUDFSuite extends AnalysisTest {
  val testRelation = LocalRelation($"a".int, $"b".double, $"c".string)

  test("SPARK-48921: ScalaUDF encoders in subquery should be resolved for MergeInto") {
    val relation =


    val string = testRelation.output(2)
    val udf = ScalaUDF((_: String) => "x", StringType, string :: Nil,
      Option(ExpressionEncoder[String]()) :: Nil)

    val mergeIntoSource =
      testRelation
        .where($"c" === udf)
        .select($"a", $"b")
        .limit(1).analyze
    val cond = mergeIntoSource.output(0) == testRelation.output(0) &&
      mergeIntoSource.output(1) == testRelation.output(1)

    val mergePlan = MergeIntoTable(
      testRelation,
      mergeIntoSource,
      cond,
      Seq(UpdateAction(None,
        Seq(Assignment(testRelation.output(0), testRelation.output(0)),
          Assignment(testRelation.output(1), testRelation.output(1)),
          Assignment(testRelation.output(2), testRelation.output(2))))),
      Seq.empty,
      Seq.empty,
      withSchemaEvolution = false).analyze

    // scalastyle:off println
    println(mergePlan)
  }
}
