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

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{AppendColumns, CatalystSerde, Expand, Filter, FlatMapGroupsWithState, LogicalGroupState, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{LongType, Metadata, StructField, StructType, TimestampType}

/**
 */
object SessionWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private final val WINDOW_COL_NAME = "session_window"
  private final val WINDOW_START = "start"
  private final val WINDOW_END = "end"

  private final val WINDOW_KEY_ATTR = "_session_window_key_"

  private def prepareKey(
      keyColumn: Expression,
      plan: LogicalPlan): (ExpressionEncoder[Row], LogicalPlan, Attribute) = keyColumn match {
    case attr: Attribute =>
      val schema = StructType.fromAttributes(Seq(attr))
      (RowEncoder(schema), plan, attr)
    case _ =>
      val keyAlias = Alias(keyColumn, WINDOW_KEY_ATTR)()
      val keyAttr = keyAlias.toAttribute
      val addedKey = Project(plan.output :+ keyAlias, plan)
      val schema = StructType.fromAttributes(Seq(keyAttr))
      (RowEncoder(schema), addedKey, keyAttr)
  }

  private def valueEncoder(value: Seq[Attribute]) = {
    val schema = StructType.fromAttributes(value)
    RowEncoder(schema)
  }

  /**
   * @param plan The logical plan
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: LogicalPlan if p.children.size == 1 && p.isStreaming =>
      val child = p.children.head
      val windowExpressions =
        p.expressions.flatMap(_.collect { case s: SessionWindow => s }).toSet

      val numWindowExpr = windowExpressions.size
      // Only support a single session window expression for now
      if (numWindowExpr == 1 &&
        windowExpressions.head.keyColumn.resolved &&
        windowExpressions.head.timeColumn.resolved &&
        windowExpressions.head.checkInputDataTypes().isSuccess) {

        val window = windowExpressions.head

        val metadata = window.timeColumn match {
          case a: Attribute => a.metadata
          case _ => Metadata.empty
        }

        val windowAttr = AttributeReference(
          WINDOW_COL_NAME, window.dataType, metadata = metadata)()

        val outputSchema = StructType.fromAttributes(child.output :+ windowAttr)
        val outputEncoder = RowEncoder(outputSchema)
        val outputDataType = outputEncoder.deserializer.dataType
        val outputNullable = !outputEncoder.clsTag.runtimeClass.isPrimitive
        val outputAttr = AttributeReference("obj", outputDataType, outputNullable)()

        val (keyEncoder, keyPlan, keyAttr) = prepareKey(window.keyColumn, child)

        val mapped = new FlatMapGroupsWithState(
          func,
          UnresolvedDeserializer(keyEncoder.deserializer, keyPlan.output)
          UnresolvedDeserializer(valueEncoder.deserializer, keyPlan.output),
          Seq(keyAttr),
          keyPlan.output,
          outputAttr,
          encoder.asInstanceOf[ExpressionEncoder[Any]],
          OutputMode.Update,
          isMapGroupsWithState = false,
          window.timeout,
          keyPlan)
        CatalystSerde.serialize[U](mapped)
      } else if (numWindowExpr > 1) {
        throw QueryCompilationErrors.multiSessionWindowExpressionsNotSupportedError(p)
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}

case class SessionWindowsState(rows: Seq[Row])

