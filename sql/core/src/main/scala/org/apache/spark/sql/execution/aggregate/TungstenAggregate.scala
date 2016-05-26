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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, DecimalType, StringType, StructType}
import org.apache.spark.unsafe.KVIterator

case class TungstenAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(TungstenAggregate.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: Seq[Attribute] =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  // This is for testing. We force TungstenAggregationIterator to fall back to the unsafe row hash
  // map and/or the sort-based aggregation once it has processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numOutputRows = longMetric("numOutputRows")
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")

    child.execute().mapPartitions { iter =>

      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {
        val aggregationIterator =
          new TungstenAggregationIterator(
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
            child.output,
            iter,
            testFallbackStartsAt,
            numOutputRows,
            peakMemory,
            spillSize)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
    }
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
    // ImperativeAggregate is not supported right now
    !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.genCode(ctx)
      val initVars = s"""
         | $isNull = ${ev.isNull};
         | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }
    val initBufVar = evaluateVariables(bufVars)

    // generate variables for output
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = functions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributes).genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, aggregateAttributes).genCode(ctx)
      }
      (resultVars, s"""
        |$evaluateAggResults
        |${evaluateVariables(resultVars)}
       """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (bufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.genCode(ctx))
      (resultVars, evaluateVariables(resultVars))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    ctx.addNewFunction(doAgg,
      s"""
         | private void $doAgg() throws java.io.IOException {
         |   // initialize aggregation buffer
         |   $initBufVar
         |
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    val numOutput = metricTerm(ctx, "numOutputRows")
    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
       | while (!$initAgg) {
       |   $initAgg = true;
       |   long $beforeAgg = System.nanoTime();
       |   $doAgg();
       |   $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
       |
       |   // output the result
       |   ${genResult.trim}
       |
       |   $numOutput.add(1);
       |   ${consume(ctx, resultVars).trim}
       | }
     """.stripMargin
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }
    ctx.currentVars = bufVars ++ input
    val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_, inputAttrs))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val aggVals = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }
    // aggregate buffer should be updated atomic
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
       """.stripMargin
    }
    s"""
       | // do aggregate
       | // common sub-expressions
       | $effectiveCodes
       | // evaluate aggregate function
       | ${evaluateVariables(aggVals)}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
     """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

  // The name for Vectorized HashMap
  private var vectorizedHashMapTerm: String = _
  private var isVectorizedHashMapEnabled: Boolean = _

  // The name for UnsafeRow HashMap
  private var hashMapTerm: String = _
  private var sorterTerm: String = _

  private case class BufferSerializer(
    expr: NamedExpression,
    ordinal: Int,
    index: Int,
    sourceDataType: DataType)

  // The name for storing domain objects for typed aggregation functions.
  private var domainObjMap: String = _
  // The serializer expressions of typed aggregation functions.
  private var bufferSerializers: Seq[BufferSerializer] = _

  /**
   * This is called by generated Java class, should be public.
   */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get().taskMemoryManager(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
    )
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }

  /**
   * Called by generated Java class to finish the aggregate and return a KVIterator.
   */
  def finishAggregate(
      hashMap: UnsafeFixedWidthAggregationMap,
      sorter: UnsafeKVExternalSorter,
      peakMemory: SQLMetric,
      spillSize: SQLMetric): KVIterator[UnsafeRow, UnsafeRow] = {

    // update peak execution memory
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val sorterMemory = Option(sorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
    val maxMemory = Math.max(mapMemory, sorterMemory)
    val metrics = TaskContext.get().taskMetrics()
    peakMemory.add(maxMemory)
    metrics.incPeakExecutionMemory(maxMemory)

    if (sorter == null) {
      // not spilled
      return hashMap.iterator()
    }

    // merge the final hashMap into sorter
    sorter.merge(hashMap.destructAndCreateExternalSorter())
    hashMap.free()
    val sortedIter = sorter.sortedIterator()

    // Create a KVIterator based on the sorted iterator.
    new KVIterator[UnsafeRow, UnsafeRow] {

      // Create a MutableProjection to merge the rows of same key together
      val mergeExpr = declFunctions.flatMap(_.mergeExpressions)
      val mergeProjection = newMutableProjection(
        mergeExpr,
        aggregateBufferAttributes ++ declFunctions.flatMap(_.inputAggBufferAttributes),
        subexpressionEliminationEnabled)
      val joinedRow = new JoinedRow()

      var currentKey: UnsafeRow = null
      var currentRow: UnsafeRow = null
      var nextKey: UnsafeRow = if (sortedIter.next()) {
        sortedIter.getKey
      } else {
        null
      }

      override def next(): Boolean = {
        if (nextKey != null) {
          currentKey = nextKey.copy()
          currentRow = sortedIter.getValue.copy()
          nextKey = null
          // use the first row as aggregate buffer
          mergeProjection.target(currentRow)

          // merge the following rows with same key together
          var findNextGroup = false
          while (!findNextGroup && sortedIter.next()) {
            val key = sortedIter.getKey
            if (currentKey.equals(key)) {
              mergeProjection(joinedRow(currentRow, sortedIter.getValue))
            } else {
              // We find a new group.
              findNextGroup = true
              nextKey = key
            }
          }

          true
        } else {
          spillSize.add(sorter.getSpillSize)
          false
        }
      }

      override def getKey: UnsafeRow = currentKey
      override def getValue: UnsafeRow = currentRow
      override def close(): Unit = {
        sortedIter.close()
      }
    }
  }

  /**
   * Generate the code for output.
   */
  private def generateResultCode(
      ctx: CodegenContext,
      keyTerm: String,
      bufferTerm: String,
      plan: String): String = {
    if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output using resultExpressions
      ctx.currentVars = null
      ctx.INPUT_ROW = keyTerm
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.INPUT_ROW = bufferTerm
      val bufferVars = aggregateBufferAttributes.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateBufferVars = evaluateVariables(bufferVars)
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributes).genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).genCode(ctx)
      }
      s"""
       $evaluateKeyVars
       $evaluateBufferVars
       $evaluateAggResults
       ${consume(ctx, resultVars)}
       """

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // This should be the last operator in a stage, we should output UnsafeRow directly
      val joinerTerm = ctx.freshName("unsafeRowJoiner")
      ctx.addMutableState(classOf[UnsafeRowJoiner].getName, joinerTerm,
        s"$joinerTerm = $plan.createUnsafeJoiner();")
      val resultRow = ctx.freshName("resultRow")
      s"""
       UnsafeRow $resultRow = $joinerTerm.join($keyTerm, $bufferTerm);
       ${consume(ctx, null, resultRow)}
       """

    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val eval = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }
      consume(ctx, eval)
    }
  }

  /**
   * Using the vectorized hash map in TungstenAggregate is currently supported for all primitive
   * data types during partial aggregation. However, we currently only enable the hash map for a
   * subset of cases that've been verified to show performance improvements on our benchmarks
   * subject to an internal conf that sets an upper limit on the maximum length of the aggregate
   * key/value schema.
   *
   * This list of supported use-cases should be expanded over time.
   */
  private def enableVectorizedHashMap(ctx: CodegenContext): Boolean = {
    val schemaLength = (groupingKeySchema ++ bufferSchema).length
    val isSupported =
      (groupingKeySchema ++ bufferSchema).forall(f => ctx.isPrimitiveType(f.dataType) ||
        f.dataType.isInstanceOf[DecimalType] || f.dataType.isInstanceOf[StringType]) &&
        bufferSchema.nonEmpty && modes.forall(mode => mode == Partial || mode == PartialMerge)

    // We do not support byte array based decimal type for aggregate values as
    // ColumnVector.putDecimal for high-precision decimals doesn't currently support in-place
    // updates. Due to this, appending the byte array in the vectorized hash map can turn out to be
    // quite inefficient and can potentially OOM the executor.
    val isNotByteArrayDecimalType = bufferSchema.map(_.dataType).filter(_.isInstanceOf[DecimalType])
      .forall(!DecimalType.isByteArrayDecimalType(_))

    isSupported  && isNotByteArrayDecimalType &&
      schemaLength <= sqlContext.conf.vectorizedAggregateMapMaxColumns
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")
    isVectorizedHashMapEnabled = enableVectorizedHashMap(ctx)
    vectorizedHashMapTerm = ctx.freshName("vectorizedHashMap")
    val vectorizedHashMapClassName = ctx.freshName("VectorizedHashMap")
    val vectorizedHashMapGenerator = new VectorizedHashMapGenerator(ctx, aggregateExpressions,
      vectorizedHashMapClassName, groupingKeySchema, bufferSchema)
    // Create a name for iterator from vectorized HashMap
    val iterTermForVectorizedHashMap = ctx.freshName("vectorizedHashMapIter")
    if (isVectorizedHashMapEnabled) {
      ctx.addMutableState(vectorizedHashMapClassName, vectorizedHashMapTerm,
        s"$vectorizedHashMapTerm = new $vectorizedHashMapClassName();")
      ctx.addMutableState(
        "java.util.Iterator<org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row>",
        iterTermForVectorizedHashMap, "")
    }

    // create hashMap
    val thisPlan = ctx.addReferenceObj("plan", this)
    hashMapTerm = ctx.freshName("hashMap")
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    ctx.addMutableState(hashMapClassName, hashMapTerm, "")
    sorterTerm = ctx.freshName("sorter")
    ctx.addMutableState(classOf[UnsafeKVExternalSorter].getName, sorterTerm, "")

    // Create a name for iterator from HashMap
    val iterTerm = ctx.freshName("mapIter")
    ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName, iterTerm, "")

    val doAgg = ctx.freshName("doAggregateWithKeys")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    ctx.addNewFunction(doAgg,
      s"""
        ${if (isVectorizedHashMapEnabled) vectorizedHashMapGenerator.generate() else ""}
        private void $doAgg() throws java.io.IOException {
          $hashMapTerm = $thisPlan.createHashMap();
          ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}

          ${if (isVectorizedHashMapEnabled) {
              s"$iterTermForVectorizedHashMap = $vectorizedHashMapTerm.rowIterator();"} else ""}

          $iterTerm = $thisPlan.finishAggregate($hashMapTerm, $sorterTerm, $peakMemory, $spillSize);
        }
       """)

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val outputCode = generateResultCode(ctx, keyTerm, bufferTerm, thisPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // The child could change `copyResult` to true, but we had already consumed all the rows,
    // so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    // Evaluate serializers for typed aggregation functions.
    // We bind serializer expressions to the array used to store domain objects.
    val objectArray = ctx.freshName("objectArray")
    ctx.currentVars = bufferSerializers.map { b =>
      val javaType = s"(${ctx.boxedType(b.sourceDataType)})"
      ExprCode("", s"$objectArray.get(${b.ordinal}) == null",
        s"($javaType$objectArray.get(${b.ordinal}))")
    }
    println(s"currentVars: ${ctx.currentVars}")
    val showData = bufferSerializers.map { b =>
      val javaType = s"(${ctx.boxedType(b.sourceDataType)})"
      s"""System.out.println("$objectArray[${b.ordinal}] = " +
        $javaType$objectArray.get(${b.ordinal}));"""
    }.mkString("\n")
    println(s"showData: $showData")
    val test = bufferSerializers.map { b =>
      b.expr.genCode(ctx).code
    }
    println(s"test: $test")
    val serializerCodes = bufferSerializers.map { b =>
      b.expr.genCode(ctx)
    }
    println(s"serializerCodes: $serializerCodes")
    val evaluatedSerializers = serializerCodes.map(_.code).mkString("\n")

    // Iterate over the aggregate rows and convert them from ColumnarBatch.Row to UnsafeRow
    def outputFromGeneratedMap: Option[String] = {
      if (isVectorizedHashMapEnabled) {
        val row = ctx.freshName("vectorizedHashMapRow")
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        var schema: StructType = groupingKeySchema
        bufferSchema.foreach(i => schema = schema.add(i))
        val generateRow = GenerateUnsafeProjection.createCode(ctx, schema.toAttributes.zipWithIndex
          .map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) })

        // Generate key for domain object map
        val generateKeyRow = GenerateUnsafeProjection.createCode(ctx, groupingKeySchema.toAttributes
          .zipWithIndex.map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) })
        val updateToRow = bufferSerializers.zip(serializerCodes).map {
          case (b, exprCode) =>
            ctx.updateColumn(generateRow.value, b.expr.dataType,
              b.index + groupingKeySchema.size, exprCode, b.expr.nullable, isVectorized = true)
        }.mkString("\n").trim

        val serializedBackToBuffer =
          if (domainObjMap != "") {
            s"""
               |   ${generateKeyRow.code}
               |   java.nio.ByteBuffer keyBytes =
                     java.nio.ByteBuffer.wrap(${generateKeyRow.value}.getBytes());
               |   System.out.println("keyBytes = " + keyBytes.hashCode());
               |   for (int i = 0; i < keyBytes.capacity(); i++) {
               |     System.out.println("keyBytes[" + i + "] = " + keyBytes.get(i));
               |   }
               |   System.out.println("size of $domainObjMap = " + $domainObjMap.size());
               |   System.out.println("unsaferow key = " + ${generateKeyRow.value});
               |   for (java.util.Iterator it = $domainObjMap.keySet().iterator(); it.hasNext();) {
               |     System.out.println("key in map = " + it.next());
               |   }
               |   if ($domainObjMap.containsKey(${generateKeyRow.value})) {
               |     System.out.println("(2) found key!");
               |     java.util.ArrayList<Object> $objectArray =
                       (java.util.ArrayList<Object>)$domainObjMap.get(${generateKeyRow.value});
               |     // Serialize back to row.
               |     $updateToRow
               |   } else {
               |     System.out.println("(2)not found key!");
               |   }
             """.stripMargin
          } else {
            ""
          }

        Option(
          s"""
             | while ($iterTermForVectorizedHashMap.hasNext()) {
             |   $numOutput.add(1);
             |   org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row $row =
             |     (org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row)
             |     $iterTermForVectorizedHashMap.next();
             |   ${generateRow.code}
             |   ${serializedBackToBuffer}
             |   ${consume(ctx, Seq.empty, {generateRow.value})}
             |
             |   if (shouldStop()) return;
             | }
             |
             | $vectorizedHashMapTerm.close();
           """.stripMargin)
      } else None
    }

    val updateToRow = bufferSerializers.zip(serializerCodes).map { case (b, exprCode) =>
      ctx.updateColumn(bufferTerm, b.expr.dataType, b.index,
        exprCode, b.expr.nullable)
    }.mkString("\n").trim
    println(s"updateToRow: $updateToRow")

    val serializedBackToBuffer =
      if (domainObjMap != "") {
        s"""
           |   java.nio.ByteBuffer keyBytes =
                 java.nio.ByteBuffer.wrap(${keyTerm}.getBytes());
           |   if ($domainObjMap.containsKey(${keyTerm})) {
           |     java.util.ArrayList<Object> $objectArray =
                   (java.util.ArrayList<Object>)$domainObjMap.get(${keyTerm});
           |     // Evaluate Serializers
           |     ${evaluatedSerializers}
           |     // Serialize back to row
           |     ${updateToRow}
           |   }
         """.stripMargin
      } else {
        ""
      }

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
     if (!$initAgg) {
       $initAgg = true;
       long $beforeAgg = System.nanoTime();
       $doAgg();
       $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
     }

     // output the result
     ${outputFromGeneratedMap.getOrElse("")}

     while ($iterTerm.next()) {
       $numOutput.add(1);
       UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
       UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
       $serializedBackToBuffer
       $outputCode

       if (shouldStop()) return;
     }

     $iterTerm.close();
     if ($sorterTerm == null) {
       $hashMapTerm.free();
     }
     """
  }

  private def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {

    // create grouping key
    ctx.currentVars = input
    val unsafeRowKeyCode = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val vectorizedRowKeys = ctx.generateExpressions(
      groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val unsafeRowKeys = unsafeRowKeyCode.value
    val unsafeRowBuffer = ctx.freshName("unsafeRowAggBuffer")
    val vectorizedRowBuffer = ctx.freshName("vectorizedAggBuffer")

    // generate hash code for key
    val hashExpr = Murmur3Hash(groupingExpressions, 42)
    ctx.currentVars = input
    val hashEval = BindReferences.bindReference(hashExpr, child.output).genCode(ctx)

    val updateExprs = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    // Special handling for TypedAggregateExpression
    val (typedAggExprs, nonTypedAggExprs) = aggregateExpressions.partition { e =>
      e.aggregateFunction match {
        case _: TypedAggregateExpression => true
        case _ => false
      }
    }

    // only have DeclarativeAggregate
    val typedUpdateExprs = typedAggExprs.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    val nonTypedUpdateExprs = nonTypedAggExprs.flatMap { e =>
      val exprs = e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
      exprs.map { expr =>
        val index = updateExprs.indexOf(expr)
        (expr, index)
      }
    }

    // println(s"typedUpdateExprs: $typedUpdateExprs")
    // println(s"nonTypedUpdateExprs: $nonTypedUpdateExprs")

    val objectArray = ctx.freshName("objectArray")

    bufferSerializers = typedAggExprs.zipWithIndex.flatMap { case (e, ordinal) =>
      val typedExpr = e.aggregateFunction.asInstanceOf[TypedAggregateExpression]
      typedExpr.bufferSerializerForCodegen(objectArray, ordinal).zipWithIndex.map {
        case (ser: NamedExpression, index: Int) =>
          val offset = e.mode match {
            case Partial | Complete =>
              updateExprs.indexOf(typedExpr.updateExpressions(index))
            case PartialMerge | Final =>
              updateExprs.indexOf(typedExpr.mergeExpressions(index))
          }
          BufferSerializer(ser, ordinal, offset, typedExpr.bufferDeserializer.dataType)
      }
    }

    println(s"bufferSerializer: $bufferSerializers")

    val bufferDeserializers = typedAggExprs.map { e =>
        e.aggregateFunction.asInstanceOf[TypedAggregateExpression].bufferDeserializer
    }

    val inputAttr = aggregateBufferAttributes ++ child.output
    ctx.currentVars = new Array[ExprCode](aggregateBufferAttributes.length) ++ input

    val boundDeserializers = bufferDeserializers.map(BindReferences.bindReference(_, inputAttr))
    // println(s"boundDeserializers: $boundDeserializers")

    ctx.INPUT_ROW = vectorizedRowBuffer
    val vectorizedBufferDeserializers = boundDeserializers.map(_.genCode(ctx))
    ctx.INPUT_ROW = unsafeRowBuffer
    val unsafeBufferDeserializers = boundDeserializers.map(_.genCode(ctx))

    // println(s"vectorizedBufferDeserializers: $vectorizedBufferDeserializers")
    // println(s"unsafeBufferDeserializers: $unsafeBufferDeserializers")

    // Rewritten TypedAggregateExpression's update/merge expressions to remove buffer serializers
    // and deserializers.
    val vectorizedRewrittenTypedUpdateExprs =
      typedAggExprs.zipWithIndex.zip(vectorizedBufferDeserializers).flatMap {
        case ((e, idx), d) =>
          e.mode match {
            case Partial | Complete =>
              e.aggregateFunction.asInstanceOf[TypedAggregateExpression]
                .updateExpressionsForCodegen(d, objectArray, idx)
            case PartialMerge | Final =>
              e.aggregateFunction.asInstanceOf[TypedAggregateExpression]
                .mergeExpressionsForCodegen(d, objectArray, idx)
          }
      }

    val unsafeRewrittenTypedUpdateExprs =
      typedAggExprs.zipWithIndex.zip(unsafeBufferDeserializers).flatMap {
        case ((e, idx), d) =>
          e.mode match {
            case Partial | Complete =>
              e.aggregateFunction.asInstanceOf[TypedAggregateExpression]
                .updateExpressionsForCodegen(d, objectArray, idx)
            case PartialMerge | Final =>
              e.aggregateFunction.asInstanceOf[TypedAggregateExpression]
                .mergeExpressionsForCodegen(d, objectArray, idx)
          }
      }
    // println(s"vectorizedRewrittenTypedUpdateExprs: $vectorizedRewrittenTypedUpdateExprs")
    // println(s"unsafeRewrittenTypedUpdateExprs: $unsafeRewrittenTypedUpdateExprs")

    // Create a map for retrieving domain objects with grouping keys.
    domainObjMap = if (typedUpdateExprs.nonEmpty) {
      val mapVariable = ctx.freshName("domainObjMap")
      ctx.addMutableState("java.util.HashMap<UnsafeRow, ArrayList<Object>>",
        mapVariable, s"$mapVariable = new java.util.HashMap<UnsafeRow, ArrayList<Object>>();")
      mapVariable
    } else {
      ""
    }

    val unsafeRowKeyCodeString = if (typedUpdateExprs.nonEmpty) {
      val code = unsafeRowKeyCode.code
      unsafeRowKeyCode.code = ""
      code
    } else {
      ""
    }

    // val updateExpr = vectorizedRewrittenTypedUpdateExprs ++ unsafeRewrittenTypedUpdateExprs ++
    //  nonTypedUpdateExprs

    val (checkFallbackForGeneratedHashMap, checkFallbackForBytesToBytesMap, resetCounter,
    incCounter) = if (testFallbackStartsAt.isDefined) {
      val countTerm = ctx.freshName("fallbackCounter")
      ctx.addMutableState("int", countTerm, s"$countTerm = 0;")
      (s"$countTerm < ${testFallbackStartsAt.get._1}",
        s"$countTerm < ${testFallbackStartsAt.get._2}", s"$countTerm = 0;", s"$countTerm += 1;")
    } else {
      ("true", "true", "", "")
    }

    // We first generate code to probe and update the vectorized hash map. If the probe is
    // successful the corresponding vectorized row buffer will hold the mutable row
    val findOrInsertInVectorizedHashMap: Option[String] = {
      if (isVectorizedHashMapEnabled) {
        Option(
          s"""
             |if ($checkFallbackForGeneratedHashMap) {
             |  ${vectorizedRowKeys.map(_.code).mkString("\n")}
             |  if (${vectorizedRowKeys.map("!" + _.isNull).mkString(" && ")}) {
             |    $vectorizedRowBuffer = $vectorizedHashMapTerm.findOrInsert(
             |        ${vectorizedRowKeys.map(_.value).mkString(", ")});
             |  }
             |}
         """.stripMargin)
      } else {
        None
      }
    }

    val updateRowInVectorizedHashMap: Option[String] = {
      if (isVectorizedHashMapEnabled) {
        ctx.INPUT_ROW = vectorizedRowBuffer
        val boundUpdateExpr =
          nonTypedUpdateExprs.map(x => BindReferences.bindReference(x._1, inputAttr))
        val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
        val effectiveCodes = subExprs.codes.mkString("\n")
        val vectorizedRowEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
          boundUpdateExpr.map(_.genCode(ctx))
        }
        val updateVectorizedRow = vectorizedRowEvals.zipWithIndex.map { case (ev, i) =>
          val dt = nonTypedUpdateExprs(i)._1.dataType
          ctx.updateColumn(vectorizedRowBuffer, dt, nonTypedUpdateExprs(i)._2, ev,
            nonTypedUpdateExprs(i)._1.nullable, isVectorized = true)
        }
        Option(
          s"""
             |// common sub-expressions
             |$effectiveCodes
             |// evaluate aggregate function
             |${evaluateVariables(vectorizedRowEvals)}
             |// update vectorized row
             |${updateVectorizedRow.mkString("\n").trim}
           """.stripMargin)
      } else None
    }

    // Next, we generate code to probe and update the unsafe row hash map.
    val findOrInsertInUnsafeRowMap: String = {
      s"""
         | if ($vectorizedRowBuffer == null) {
         |   // generate grouping key
         |   ${unsafeRowKeyCode.code.trim}
         |   ${hashEval.code.trim}
         |   if ($checkFallbackForBytesToBytesMap) {
         |     // try to get the buffer from hash map
         |     $unsafeRowBuffer =
         |       $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, ${hashEval.value});
         |   }
         |   if ($unsafeRowBuffer == null) {
         |     if ($sorterTerm == null) {
         |       $sorterTerm = $hashMapTerm.destructAndCreateExternalSorter();
         |     } else {
         |       $sorterTerm.merge($hashMapTerm.destructAndCreateExternalSorter());
         |     }
         |     $resetCounter
         |     // the hash map had be spilled, it should have enough memory now,
         |     // try  to allocate buffer again.
         |     $unsafeRowBuffer =
         |       $hashMapTerm.getAggregationBufferFromUnsafeRow($unsafeRowKeys, ${hashEval.value});
         |     if ($unsafeRowBuffer == null) {
         |       // failed to allocate the first page
         |       throw new OutOfMemoryError("No enough memory for aggregation");
         |     }
         |   }
         | }
       """.stripMargin
    }

    val updateRowInUnsafeRowMap: String = {
      ctx.INPUT_ROW = unsafeRowBuffer
      val boundUpdateExpr =
        nonTypedUpdateExprs.map(x => BindReferences.bindReference(x._1, inputAttr))
      val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
      val effectiveCodes = subExprs.codes.mkString("\n")
      val unsafeRowBufferEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
        boundUpdateExpr.map(_.genCode(ctx))
      }
      val updateUnsafeRowBuffer = unsafeRowBufferEvals.zipWithIndex.map { case (ev, i) =>
        val dt = nonTypedUpdateExprs(i)._1.dataType
        ctx.updateColumn(unsafeRowBuffer, dt, nonTypedUpdateExprs(i)._2, ev,
          nonTypedUpdateExprs(i)._1.nullable)
      }
      s"""
         |// common sub-expressions
         |$effectiveCodes
         |// evaluate aggregate function
         |${evaluateVariables(unsafeRowBufferEvals)}
         |// update unsafe row buffer
         |${updateUnsafeRowBuffer.mkString("\n").trim}
           """.stripMargin
    }

    // Generate key for domain object map
    // val keepVars = ctx.currentVars
    // ctx.currentVars = null
    // ctx.INPUT_ROW = vectorizedRowBuffer
    val generateKeyRow = GenerateUnsafeProjection.createCode(ctx, groupingKeySchema.toAttributes
      .zipWithIndex.map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) })
    val genKeyForDomainObjMap = if (isVectorizedHashMapEnabled) {
      generateKeyRow.code
    } else {
      ""
    }

    // ctx.currentVars = keepVars

    // Next, we generate code to probe and update the domain object map.
    val findOrInsertInDomainObjMap: String = {
      if (typedUpdateExprs.nonEmpty) {
        s"""
           | java.nio.ByteBuffer keyBytes = java.nio.ByteBuffer.wrap($unsafeRowKeys.getBytes());
           | java.util.ArrayList<Object> $objectArray = null;
           | System.out.println("keyBytes = " + keyBytes.hashCode());
           | for (int i = 0; i < keyBytes.capacity(); i++) {
           |   System.out.println("keyBytes[" + i + "] = " + keyBytes.get(i));
           | }
           | System.out.println("unsaferow key = " + $unsafeRowKeys);
           | for (java.util.Iterator it = $domainObjMap.keySet().iterator(); it.hasNext();) {
           |   System.out.println("key in map = " + it.next());
           | }
           | if ($domainObjMap.containsKey($unsafeRowKeys)) {
           |   System.out.println("Found key!");
           |   $objectArray = (java.util.ArrayList<Object>)$domainObjMap.get($unsafeRowKeys);
           | } else {
           |   System.out.println("Not found key!");
           |   // Get domain object and put into map.
           |   $objectArray = new java.util.ArrayList<Object>();
           |   if ($vectorizedRowBuffer != null) {
           |     ${vectorizedBufferDeserializers.map(_.code).mkString("\n")}
           |     ${vectorizedBufferDeserializers.map(d => s"$objectArray.add(${d.value});")
                   .mkString("\n")}
           |   } else {
           |     ${unsafeBufferDeserializers.map(_.code).mkString("\n")}
           |     ${unsafeBufferDeserializers.map(d => s"$objectArray.add(${d.value});")
                   .mkString("\n")}
           |   }
           |   $domainObjMap.put($unsafeRowKeys.copy(), $objectArray);
           |   ${unsafeBufferDeserializers.zipWithIndex.map { case (_, idx) =>
                s"""System.out.println("init value at " + $idx + " = " + $objectArray.get($idx));"""
               }.mkString("\n")}
           | }
           | System.out.println("size of $domainObjMap = " + $domainObjMap.size());
         """.stripMargin
      } else {
        ""
      }
    }

    // We only need to use unsafeRewrittenTypedUpdateExprs here.
    val rewrittenTypedUpdateExprs =
      unsafeRewrittenTypedUpdateExprs.map(BindReferences.bindReference(_, inputAttr).genCode(ctx))
    val domainObjUpdateExprs = rewrittenTypedUpdateExprs.zipWithIndex.map { case (expr, idx) =>
      s"""System.out.println("before update at " + $idx + " = " + $objectArray.get($idx));""" +
      s"""System.out.println("index = " + $idx + " value = " + ${expr.value});\n""" +
      s"$objectArray.set($idx, ${expr.value}); \n" +
      s"""System.out.println("before update at " + $idx + " = " + $objectArray.get($idx));"""
    }
    // Generate codes used to update domain objects.
    val updateDomainObjects: String = {
      if (typedUpdateExprs.nonEmpty) {
        s"""
           | ${rewrittenTypedUpdateExprs.map(_.code).mkString("\n")}
           | ${domainObjUpdateExprs.mkString("\n")}
         """.stripMargin
       } else {
        ""
      }
    }

    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    s"""
     UnsafeRow $unsafeRowBuffer = null;
     org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row $vectorizedRowBuffer = null;

     $unsafeRowKeyCodeString

     ${findOrInsertInVectorizedHashMap.getOrElse("")}

     $findOrInsertInUnsafeRowMap

     $findOrInsertInDomainObjMap

     $incCounter

     if ($vectorizedRowBuffer != null) {
       // update vectorized row
       ${updateRowInVectorizedHashMap.getOrElse("")}
     } else {
       // update unsafe row
       $updateRowInUnsafeRowMap
     }

     $updateDomainObjects
     """
  }

  override def simpleString: String = {
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = groupingExpressions.mkString("[", ",", "]")
        val functionString = allAggregateExpressions.mkString("[", ",", "]")
        val outputString = output.mkString("[", ",", "]")
        s"TungstenAggregate(key=$keyString, functions=$functionString, output=$outputString)"
      case Some(fallbackStartsAt) =>
        s"TungstenAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }
}

object TungstenAggregate {
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)
    UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
  }
}
