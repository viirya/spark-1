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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Collect a list of elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2,1]
  """,
  since = "2.0.0")
case class CollectList(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = ArrayType(child.dataType)

  override lazy val deterministic: Boolean = false

  private lazy val collectedList = AttributeReference("collectedList", dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = collectedList :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* collectedList = */ Literal.default(dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* collectedList = */ If(IsNull(child),
      collectedList,
      Concat(Seq(collectedList, CreateArray(Seq(child)))))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* collectedList = */ Concat(Seq(collectedList.left, collectedList.right))
    )
  }

  override lazy val evaluateExpression: AttributeReference = collectedList
}

/**
 * Collect a set of unique elements.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (1) AS tab(col);
       [1,2]
  """,
  since = "2.0.0")
case class CollectSet(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = ArrayType(child.dataType)

  override lazy val deterministic: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.dataType.existsRecursively(_.isInstanceOf[MapType])) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure("collect_set() cannot have map type data")
    }
  }

  private lazy val collectedSet = AttributeReference("collectedSet", dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = collectedSet :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* collectedSet = */ Literal.default(dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* collectedSet = */ If(IsNull(child),
      collectedSet,
      ArrayUnion(collectedSet, CreateArray(Seq(child))))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* collectedSet = */ ArrayUnion(collectedSet.left, collectedSet.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = collectedSet
}
