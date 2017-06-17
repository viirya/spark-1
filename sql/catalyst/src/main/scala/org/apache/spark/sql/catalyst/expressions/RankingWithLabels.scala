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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.{LambdaVariable, MapObjects}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, StructField, StructType}

case class RankingWithLabels(
    items: Expression,
    labels: Expression,
    scores: Expression,
    randomize: Boolean = true)
  extends TernaryExpression with CodegenFallback {

  override val doCodegenInChildren: Boolean = true

  private lazy val modelNames = scores.dataType.asInstanceOf[ArrayType]
    .elementType.asInstanceOf[StructType].map(_.name).toArray
  private lazy val itemType: DataType = items.dataType.asInstanceOf[ArrayType].elementType
  private lazy val labelType: DataType = labels.dataType.asInstanceOf[ArrayType].elementType
  private lazy val scoreType: DataType = {
    val scoreTypes: Seq[DataType] = scores.dataType.asInstanceOf[ArrayType].elementType
      .asInstanceOf[StructType].map(field => field.dataType)
    require(scoreTypes.toSet.size == 1, "All the types in score fields should be the same.")

    scoreTypes.head match {
      case DoubleType =>
      case FloatType =>
      case _ =>
        throw new IllegalArgumentException("The score type has to be Double or Float, but found " +
          s"${scoreType.typeName}")
    }
    scoreTypes.head
  }
  override def deterministic: Boolean = randomize

  override def dataType: StructType = StructType(
    modelNames.map { name =>
      StructField(name, StructType(
        Array(
          StructField("ranking", ArrayType(itemType, containsNull = false), nullable = false),
          StructField("labels", ArrayType(labelType, containsNull = false), nullable = false)
        )
      ), nullable = false)
    }
  )

  override def children: Seq[Expression] = items :: labels :: scores :: Nil

  override def ternaryCodegenEval(input1: Any, input2: Any, input3: Any): Any = {
    println("call ternaryCodegenEval")
    if (input1 == null || input2 == null || input3 == null) {
      null
    } else {
      nullSafeEval(input1, input2, input3)
    }
  }

  override def nullSafeEval(items: Any, labels: Any, scores: Any): Any = {
    InternalRow.fromSeq(
      Functions.computeFullRanksAndLabels(
        modelNames,
        items.asInstanceOf[ArrayData].array,
        scores.asInstanceOf[ArrayData].array,
        scoreType,
        Some(labels.asInstanceOf[ArrayData].array),
        randomize
      ).map {
        case (sortedItemArray: Seq[Any], Some(sortedLabelArray: Seq[Any])) =>
          InternalRow.fromSeq(Seq(new GenericArrayData(sortedItemArray),
            new GenericArrayData(sortedLabelArray)))
        case _ => throw new IllegalArgumentException("Missing label columns.")
      }
    )
  }
}

object Functions {
  def computeFullRanksAndLabels(
                                 modelNames: Seq[String],
                                 itemArray: Seq[Any],
                                 scoreArray: Seq[Any],
                                 scoreType: DataType,
                                 labelArray: Option[Seq[Any]],
                                 randomize: Boolean
                               ): Seq[(Seq[Any], Option[Seq[Any]])] = {

    val itemAndLabelArray: Seq[(Any, Any)] = {
      labelArray match {
        case Some(labelArray: Seq[Any]) =>
          require(labelArray.length == itemArray.length,
            s"The number of item ${itemArray.length} doesn't match " +
            s"the number of labels for those items ${labelArray.length}.")
          itemArray.zip(labelArray)
        case None =>
          itemArray.map(x => (x, null))
      }
    }

    val numModels = modelNames.length
    val numOfItems = itemAndLabelArray.length

    val scoreMatrix = {
      val result = Array.ofDim[Double](numModels, numOfItems)
      require(scoreArray.length == numOfItems)
      var i = 0
      scoreArray.foreach { case x: InternalRow =>
        var j = 0
        while (j < numModels) {
          result(j)(i) = scoreType match {
            case DoubleType => x.getDouble(j)
            case FloatType => x.getFloat(j).toDouble
          }
          j += 1
        }
        i += 1
      }
      result
    }

    scoreMatrix.map { scores =>
      val sortedItemLabelAndScoreArray: Seq[((Any, Any), Double)] = if (randomize) {
        import scala.collection.JavaConverters._
        val toBeShuffled =
          java.util.Arrays.asList(itemAndLabelArray.zip(scores).toArray.clone(): _*)
        java.util.Collections.shuffle(toBeShuffled)
        toBeShuffled.asScala.sortBy(-_._2)
      } else {
        itemAndLabelArray.zip(scores).sortBy(-_._2)
      }
      val itemArray = sortedItemLabelAndScoreArray.map { case ((item, _), _) => item }

      labelArray match {
        case Some(_) =>
          (itemArray, Some(sortedItemLabelAndScoreArray.map { case ((_, label), _) => label }))
        case None =>
          (itemArray, None)
      }
    }
  }
}
