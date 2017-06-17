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

package org.apache.spark.ml

import scala.util.{Success, Try}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.{LambdaVariable, MapObjects}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.functions.{struct, udf}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, StructField, StructType}

case class Item(show_title_id: Int, label: Double, weight: Double, features: Features)

case class Features(feature1: Double, feature2: Double)

case class Datum(profileId: Long, country: String, items: Array[Item])

class DebuggingSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  lazy val inputDataDMC12: DataFrame = {
    val sqlImport = spark.sqlContext
    import sqlImport.implicits._
    Seq(
      Datum(216355768330191130L, "US",
        Array(
          Item(1, 1.0, 1.0, Features(1.0, 0.0)),
          Item(2, 0.0, 1.0, Features(0.1, 0.2)),
          Item(3, 0.0, 1.0, Features(0.4, 0.3)),
          Item(4, 0.0, 1.0, Features(0.2, 0.1)),
          Item(5, 0.0, 1.0, Features(0.1, 0.1))
        )
      ),
      Datum(1340843238L, "US",
        Array(
          Item(1, 0.0, 1.0, Features(0.0, 0.0)),
          Item(2, 1.0, 1.0, Features(0.0, 1.0)),
          Item(3, 0.0, 1.0, Features(0.0, 0.0)),
          Item(4, 0.0, 1.0, Features(0.0, 0.0)),
          Item(5, 0.0, 1.0, Features(0.0, 0.0))
        )
      )
    ).toDF
  }

  test("pipeline") {
    val predictor = new NewPredictor()
    val scores = predictor.transform(inputDataDMC12) // .cache()
    scores.show()

    val ranking = scores.withColumn("ranking", WithExpr(
      RankingWithLabels(
        WithColumn(scores("items.show_title_id")),
        WithColumn(scores("items.label")),
        WithColumn(scores("items.scores"))
      )
    ))
    println(ranking.queryExecution.executedPlan)
    ranking.explain(true)
    ranking.show()
  }
}

object Functions {
  def eachItem(itemsCol: Column)(fn: Column => Column): Column = mkArrayExpression(
    "eachItem",
    MapObjects.test,
    itemsCol, fn
  )

  def mkArrayExpression(
      name: String,
      mkExpr: ((Expression => Expression, Expression, DataType) => Expression),
      itemsCol: Column,
      itemFn: Column => Column): Column = WithExpr {
    val itemsExpr = WithColumn(itemsCol)
    val itemExprFunction = (WithExpr.apply _) andThen itemFn andThen (WithColumn.apply _)
    mkExpr(itemExprFunction, itemsExpr, resolvedElementType(name, itemsExpr))
  }

  // tries to resolve the data type that will be output by the expression
  def baseType(expr: Expression): Try[DataType] = expr match {
    case MapObjects(_, _, _, lf, _, _) => baseType(lf).map(ArrayType(_))
    // case FilterObjects(_, _, _, items, _) => baseType(items)
    case LambdaVariable(_, _, elementType, _) => Success(elementType)
    case other => Try(other.dataType)
  }

  // tries to create a copy of the expression where the data types can be resolved
  def baseExpr(expr: Expression): Expression = expr.transformUp {
    case UnresolvedAlias(child, Some(aliasFn)) => Alias(child, aliasFn(child))()
    case UnresolvedExtractValue(child, extraction) => ExtractValue(child, extraction, _ == _)
  }

  def resolvedElementType(func: String, expr: Expression): DataType = {
    resolvedType(expr) match {
      case Success(ArrayType(elementType, _)) => elementType
      case Success(other) =>
        throw new IllegalArgumentException(s"$func expects an ArrayType; was passed $other")
      case _ =>
        throw new IllegalArgumentException(s"Cannot call $func with unresolved expression $expr")
    }
  }

  def resolvedType(expr: Expression): Try[DataType] = baseType(baseExpr(expr))
}

/**
 * @param uid uid of this transformer
 */
class NewPredictor(override val uid: String = "aaa") extends Transformer {

  private val predictorWrapperModels: Seq[PredictorWrapper] =
    Seq(new PredictorWrapper("model", Seq("feature1", "feature2"), (f: Array[Double]) => f.sum))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputSchema = dataset.schema
    transformSchema(inputSchema)
    val itemsColName = "items"
    val featureColName = "features"
    val modelNames = predictorWrapperModels.map(model => model.modelName)

    val isDMC15: Boolean = detectDMC15(inputSchema)

    // In DMC15, there is no feature name in schema, we will check when we pull the data out
    // from the feature map
    val providedFeatureNames: Option[Seq[String]] = if (isDMC15) {
      None
    } else {
      Some(inputSchema(itemsColName).dataType.asInstanceOf[ArrayType]
        .elementType.asInstanceOf[StructType](featureColName)
        .dataType.asInstanceOf[StructType].map(_.name))
    }

    // Now, we are going to build the feature map from feature name to index.
    // It's executed in driver, and will be only executed once.
    // I was thinking to optimize it, but in the end, don't want to do premature optimization.
    val predictorsWithFeatureIndices: Option[Seq[(PredictorWrapper, Array[Int])]] = {
      providedFeatureNames match {
        case Some(names) =>
          Some(predictorWrapperModels.map(model =>
            (model, model.featureNames.map(name => names.indexOf(name)).toArray)))
        case None =>
          None
      }
    }

    // For serialization to have a local copy
    val localPredictorWrapperModels = predictorWrapperModels.toArray

    val itemsWithScores: Column = Functions.eachItem(dataset(itemsColName)) { item: Column =>
      val columns: Seq[Column] = FlattenStructColumn(item)

      val featureColumn: Column = {
        val temp = columns.filter(col => GetColumnName(col) == featureColName)
        require(temp.length == 1)
        temp.head
      }

      val scoreColumnType: StructType = StructType(
        modelNames.map(name => StructField(name, DoubleType, nullable = false))
      )

      val scoringFun: Row => Row = (row: Row) => Row.fromSeq {
        predictorsWithFeatureIndices match {
          case Some(x) => x.map { case (model, indices) =>
            // TODO: Abstract this out for different data format.
            val features: Array[Double] = indices.map(row(_) match {
              case value: Double => value
              case value => throw new IllegalArgumentException("Unsupported feature type, " +
                s"${value.getClass.toString}")
            })
            model.predictingFunction(features)
          }
          // For supporting DMC15
          case None =>
            val featureMap: Map[String, Double] = row.getAs[Map[String, Double]]("double_features")
            localPredictorWrapperModels.map { model =>
              val features: Array[Double] = {
                val featureNames = model.featureNames
                featureNames.map(name => featureMap.get(name) match {
                  case Some(value) => value
                  case None =>
                    throw new IllegalArgumentException(s"The model, ${model.modelName} requires " +
                      s"$name, but this feature is not in the input dataframe.")
                }).toArray
              }
              model.predictingFunction(features)
            }
        }
      }

      val scoreColumn = udf(scoringFun, scoreColumnType)(featureColumn) as "scores"
      val indexOfExistingScores = columns.indexWhere(col => GetColumnName(col) == "scores")
      if (indexOfExistingScores != -1) {
        struct(columns.updated(indexOfExistingScores, scoreColumn): _*)
      } else {
        struct(columns ++: Seq(scoreColumn): _*)
      }
    }

    dataset.withColumn(s"items_with_scores_$uid", itemsWithScores).drop(itemsColName)
      .withColumnRenamed(s"items_with_scores_$uid", itemsColName)
  }

  private def detectDMC15(schema: StructType): Boolean = {
    val itemStructField = schema("items")
    val itemsStructType =
      itemStructField.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val featuresStructType = itemsStructType("features").dataType.asInstanceOf[StructType]
    if (featuresStructType.fieldNames.contains("double_features")) {
      true
    } else {
      false
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

/**
 * It's a helper model which constructs a simple predictor with anonymous function
 * which will eventually be used in UDF. (Currently UDF doesn't support nested data structure).
 * We could make it private, but I think the side effect of having this helper model is
 * we can easily try different predictors as long as the predictors can be wrapped
 * into the simple anonymous function.
 */
case class PredictorWrapper(
    modelName: String,
    featureNames: Seq[String],
    predictingFunction: Array[Double] => Double)
