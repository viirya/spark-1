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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.metric.SQLMetrics

/** Base physical plan for [[FilterExec]] and [[StopAfter]]. */
abstract class FilterExecBase(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a: NullIntolerant) if a.references.subsetOf(child.outputSet) => true
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    ctx.currentVars = input

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = "false"
      }
      ev
    }

    s"""
       |${stopEarly(ctx)}
       |$generated
       |$nullChecks
       |$numOutput.add(1);
       |${recordCondPass}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  // Two helper functions used to generate codes for stopping filtering early.
  protected def stopEarly(ctx: CodegenContext): String
  protected def recordCondPass: String

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
    extends FilterExecBase(condition, child) {

  override protected def stopEarly(ctx: CodegenContext): String = ""
  override protected def recordCondPass = ""

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      val predicate = newPredicate(condition, child.output)
      iter.filter { row =>
        val r = predicate(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }
}

/**
 *  Specific physical plan for Filter. Different to [[FilterExec]], this plan is specified for
 *  filtering on sorted data. If the data is sorted by a key, filters on the key could stop as
 *  soon as the data is out of range. For example, when we do filtering on a data sorted by a
 *  column called "id". The filter condition WHERE id < 10 should stop as soon as the first
 *  row with the column "id" equal to or more than 10 is seen.
 */
case class StopAfter(condition: Expression, child: SparkPlan)
    extends FilterExecBase(condition, child) {

  // An internal flag in generated codes to record whether filtering condition is passed
  // in previous run. If it is false, we know we can stop this filtering early.
  private var condPassed: String = ""

  override protected def stopEarly(ctx: CodegenContext): String = {
    condPassed = ctx.freshName("condPassed")
    ctx.addMutableState(s"boolean", condPassed, s"$condPassed = true;")
    s"""
       |if (!$condPassed) break;
       |$condPassed = false;
     """.stripMargin
  }

  override protected def recordCondPass = s"$condPassed = true;"

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      val predicate = newPredicate(condition, child.output)
      iter.takeWhile { row =>
        val r = predicate(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }
}

object FilterExecBase extends PredicateHelper {
  /**
   * A boolean function which determines whether a physical plan for filtering can be executed
   * by a [[StopAfter]]. If the child output is sorted, we can determine to stop filtering as
   * early as possible.
   * E.g, when we do filtering with "WHERE id < 10" on a child sorted by id in ascending. Once
   * the condition is false, it guarantees that no more data will satisfy the condition and we
   * can stop this filtering early.
   */
  def shouldBeStopAfter(condition: Expression, child: SparkPlan): Boolean = {
    // Take the prefix of attributes that the child output is ordered by.
    // E.g., if the child.outputOrdering is [a, b, c + 1, d], the prefix is [a, b].
    val orderByPrefixAttrs = child.outputOrdering.map(_.child).takeWhile { e =>
      if (e.isInstanceOf[Attribute]) true else false
    }
    val orderByPrefixAttrsIds = orderByPrefixAttrs.map(_.asInstanceOf[Attribute].exprId)

    // If the condition's attributes are subset of order by attributes.
    // And these attributes are not nullable.
    // TODO: Can we utilize `NullOrdering` to adapt for nullable attributes?
    val condAttrsAreSortOrderAndNotNull = condition.references.baseSet.map(_.a).forall { attr =>
      orderByPrefixAttrsIds.contains(attr.exprId) &&
        !attr.nullable
    }

    val sortOrders = child.outputOrdering.take(orderByPrefixAttrs.length)
    val (ascOrders, descOrders) = sortOrders.partition(_.isAscending)
    val onlyOneSortOrderDirection = ascOrders.isEmpty || descOrders.isEmpty

    val (binaryComparison, notBinaryComparison) =
      splitConjunctivePredicates(condition).partition(_.isInstanceOf[BinaryComparison])

    // Normalize binary comparison. E.g., 1 < id will be reordered to id > 1.
    val reordered = normalizePredicate(binaryComparison)

    val binaryComparisonBetweenAttrAndLiteral = reordered.forall { expr =>
      expr.asInstanceOf[BinaryComparison].left.isInstanceOf[Attribute] &&
        expr.asInstanceOf[BinaryComparison].right.isInstanceOf[Literal]
    }

    val deterministic = reordered.forall(_.deterministic)

    val sameDirectionPredicates = isAllowedPredicatesInSameDirection(reordered)

    // Some requirements should be satisfied in order to use [[StopAfter]]. Otherwise, the filtered
    // results might be incorrect.
    //  1. The child plan is ordered by some prefix attributes.
    //  2. Filter condition 's attributes are subset of the prefix order by attributes.
    //  3. Filter condition 's attributes are not nullable.
    //  4. All condition expressions are [[BinaryComparison]] and deterministic.
    //  5. These [[BinaryComparison]] should compare prefix order by attribute and literal.
    //  6. These [[BinaryComparison]] should be the same direction.
    //  7. The sorting direction for all prefix order by attributes must be the same.
    if (orderByPrefixAttrs.nonEmpty && condAttrsAreSortOrderAndNotNull &&
        notBinaryComparison.isEmpty && deterministic &&
        binaryComparisonBetweenAttrAndLiteral &&
        sameDirectionPredicates &&
        onlyOneSortOrderDirection) {

      if (!ascOrders.isEmpty) {
        // If the child output is sorted in ascending order.
        reordered.head match {
          case _: LessThan => true
          case _: LessThanOrEqual => true
          case _ => false
        }
      } else {
        // If the child output is sorted in descending order.
        reordered.head match {
          case _: GreaterThan => true
          case _: GreaterThanOrEqual => true
          case _ => false
        }
      }
    } else {
      false
    }
  }

  /**
   * Normalizes a sequence of predicates to move literal to right child of predicate.
   */
  private def normalizePredicate(predicates: Seq[Expression]): Seq[Expression] = {
    // TODO: Extract attribute from simple arithmetic expression.
    // E.g., a + 1 > 5 can be normalized to a > 4.
    // We can add normalization like this to make [[StopAfter]] more flexible.
    predicates.map { expr =>
      expr match {
        case GreaterThan(v @ NonNullLiteral(_, _), r) => LessThan(r, v)
        case LessThan(v @ NonNullLiteral(_, _), r) => GreaterThan(r, v)
        case GreaterThanOrEqual(v @ NonNullLiteral(_, _), r) => LessThanOrEqual(v, r)
        case LessThanOrEqual(v @ NonNullLiteral(_, _), r) => GreaterThanOrEqual(v, r)
        case _ => expr
      }
    }
  }

  /**
   *  The binary comparison must be the same direction. I.e.,
   *    1. All comparison are LessThan or LessThanOrEqual.
   *    2. All comparison are GreaterThan or GreaterThanOrEqual.
   */
  private def isAllowedPredicatesInSameDirection(predicates: Seq[Expression]): Boolean = {
    if (predicates.length > 0) {
      predicates.head match {
        case _: LessThan | _: LessThanOrEqual =>
          predicates.tail.collect {
            case _: GreaterThan | _: GreaterThanOrEqual => true
            case _: EqualTo | _: EqualNullSafe => true
          }.isEmpty
        case _: GreaterThan | _: GreaterThanOrEqual =>
          predicates.tail.collect {
            case _: LessThan | _: LessThanOrEqual => true
            case _: EqualTo | _: EqualNullSafe => true
          }.isEmpty
        case _ => false
      }
    } else {
      false
    }
  }
}

/**
 * Replaces [[FilterExec]] in a query plan with [[StopAfter]] if its child plan is satisfying
 * the necessary requirements.
 */
case object ApplyStopAfter extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = plan.transform {
    case FilterExec(condition, child) if FilterExecBase.shouldBeStopAfter(condition, child) =>
      StopAfter(condition, child)
  }
}
