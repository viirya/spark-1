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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._

/**
 * A trait that can be used to provide a fallback mode for expression code generation.
 */
trait CodegenFallback extends Expression {

  // If the expression extending `CodegenFallback` is unary, binary or ternary expression, we
  // support to evaluate its children expressions by codegen. The expression needs to override
  // `doCodegenInChildren` to true, and override the evaluation method for codegen in following.
  val doCodegenInChildren: Boolean = false

  def unaryCodegenEval(input: Any): Any =
    throw new UnsupportedOperationException("Doesn't support codegen in children expressions")

  def binaryCodegenEval(input1: Any, input2: Any): Any =
    throw new UnsupportedOperationException("Doesn't support codegen in children expressions")

  def ternaryCodegenEval(input1: Any, input2: Any, input3: Any): Any =
    throw new UnsupportedOperationException("Doesn't support codegen in children expressions")

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val idx = ctx.references.length
    ctx.references += this
    if (doCodegenInChildren) {
      doGenCodeWithChildren(idx, ctx, ev)
    } else {
      doGenCodeWithoutChildren(idx, ctx, ev)
    }
  }

  private def doGenCodeWithChildren(exprIdx: Int, ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)

    val codegenEvalMethod = this match {
      case _: UnaryExpression => "unaryCodegenEval"
      case _: BinaryExpression => "binaryCodegenEval"
      case _: TernaryExpression => "ternaryCodegenEval"
      case _ =>
        throw new UnsupportedOperationException("`doCodegenInChildren` only works for up " +
          "to three children")
    }

    val childrenEvals = this.children.map(_.genCode(ctx))
    val childrenCodes = childrenEvals.map(_.code).mkString("\n")
    val callParams = childrenEvals.map(_.value).mkString(", ")

    val fallbackClass = "org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback"

    val eval = s"""
      $childrenCodes
      $placeHolder
      Object $objectTerm = (($fallbackClass) references[$exprIdx]).$codegenEvalMethod($callParams);
    """
    if (nullable) {
      ev.copy(s"""
        $eval
        boolean ${ev.isNull} = $objectTerm == null;
        ${ctx.javaType(this.dataType)} ${ev.value} = ${ctx.defaultValue(this.dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = (${ctx.boxedType(this.dataType)}) $objectTerm;
        }""")
    } else {
      ev.copy(s"""
        $eval
        ${ctx.javaType(this.dataType)} ${ev.value} = (${ctx.boxedType(this.dataType)}) $objectTerm;
        """, isNull = "false")
    }
  }

  private def doGenCodeWithoutChildren(
      exprIdx: Int,
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    // LeafNode does not need `input`
    val input = if (this.isInstanceOf[LeafExpression]) "null" else ctx.INPUT_ROW
    var childIndex = exprIdx
    this.foreach {
      case n: Nondeterministic =>
        // This might add the current expression twice, but it won't hurt.
        ctx.references += n
        childIndex += 1
        ctx.addPartitionInitializationStatement(
          s"""
             |((Nondeterministic) references[$childIndex])
             |  .initialize(partitionIndex);
          """.stripMargin)
      case _ =>
    }
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)
    if (nullable) {
      ev.copy(code = s"""
        $placeHolder
        Object $objectTerm = ((Expression) references[$exprIdx]).eval($input);
        boolean ${ev.isNull} = $objectTerm == null;
        ${ctx.javaType(this.dataType)} ${ev.value} = ${ctx.defaultValue(this.dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = (${ctx.boxedType(this.dataType)}) $objectTerm;
        }""")
    } else {
      ev.copy(code = s"""
        $placeHolder
        Object $objectTerm = ((Expression) references[$exprIdx]).eval($input);
        ${ctx.javaType(this.dataType)} ${ev.value} = (${ctx.boxedType(this.dataType)}) $objectTerm;
        """, isNull = "false")
    }
  }
}
