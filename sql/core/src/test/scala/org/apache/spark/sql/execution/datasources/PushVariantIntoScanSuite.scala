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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant._
import org.apache.spark.sql.catalyst.plans.logical._
// BEGIN-V2-SUPPORT: DataSource V2 imports for tests (commented due to incomplete reader support)
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
// END-V2-SUPPORT
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class PushVariantIntoScanSuiteBase extends SharedSparkSession {
  override def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.PUSH_VARIANT_INTO_SCAN.key, "true")

  protected def localTimeZone = spark.sessionState.conf.sessionLocalTimeZone

  // Return a `StructField` with the expected `VariantMetadata`.
  protected def field(ordinal: Int, dataType: DataType, path: String,
                    failOnError: Boolean = true, timeZone: String = localTimeZone): StructField =
    StructField(ordinal.toString, dataType,
      metadata = VariantMetadata(path, failOnError, timeZone).toMetadata)

  // Validate an `Alias` expression has the expected name and child.
  protected def checkAlias(expr: Expression, expectedName: String, expected: Expression): Unit = {
    expr match {
      case Alias(child, name) =>
        assert(name == expectedName)
        assert(child == expected)
      case _ => fail()
    }
  }

}

// V1 DataSource tests
class PushVariantIntoScanSuite extends PushVariantIntoScanSuiteBase {
  private def testOnFormats(fn: String => Unit): Unit = {
    for (format <- Seq("PARQUET")) {
      test("test - " + format) {
        withTable("T") {
          fn(format)
        }
      }
    }
  }

  testOnFormats { format =>
    sql("create table T (v variant, vs struct<v1 variant, v2 variant, i int>, " +
      "va array<variant>, vd variant default parse_json('1'), s string) " +
      s"using $format")

    sql("select variant_get(v, '$.a', 'int') as a, v, cast(v as struct<b float>) as v from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        checkAlias(projectList(1), "v", GetStructField(v, 1))
        checkAlias(projectList(2), "v", GetStructField(v, 2))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, VariantType, "$", timeZone = "UTC"),
          field(2, StructType(Array(StructField("b", FloatType))), "$"))))
      case _ => fail()
    }

    // Validate _metadata works.
    sql("select variant_get(v, '$.a', 'int') as a, _metadata from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        assert(projectList(1).dataType.isInstanceOf[StructType])
      case _ => fail()
    }

    sql("select 1 from T where isnotnull(v)")
      .queryExecution.optimizedPlan match {
      case Project(projectList, Filter(condition, l: LogicalRelation)) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "1", Literal(1))
        assert(condition == IsNotNull(v))
        assert(v.dataType == StructType(Array(
          field(0, BooleanType, "$.__placeholder_field__", failOnError = false, timeZone = "UTC"))))
      case _ => fail()
    }

    sql("select variant_get(v, '$.a', 'int') + 1 as a, try_variant_get(v, '$.b', 'string') as b " +
      "from T where variant_get(v, '$.a', 'int') = 1").queryExecution.optimizedPlan match {
      case Project(projectList, Filter(condition, l: LogicalRelation)) =>
        val output = l.output
        val v = output(0)
        checkAlias(projectList(0), "a", Add(GetStructField(v, 0), Literal(1)))
        checkAlias(projectList(1), "b", GetStructField(v, 1))
        assert(condition == And(IsNotNull(v), EqualTo(GetStructField(v, 0), Literal(1))))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, StringType, "$.b", failOnError = false))))
      case _ => fail()
    }

    sql("select variant_get(vs.v1, '$.a', 'int') as a, variant_get(vs.v1, '$.b', 'int') as b, " +
      "variant_get(vs.v2, '$.a', 'int') as a, vs.i from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vs = output(1)
        val v1 = GetStructField(vs, 0, Some("v1"))
        val v2 = GetStructField(vs, 1, Some("v2"))
        checkAlias(projectList(0), "a", GetStructField(v1, 0))
        checkAlias(projectList(1), "b", GetStructField(v1, 1))
        checkAlias(projectList(2), "a", GetStructField(v2, 0))
        checkAlias(projectList(3), "i", GetStructField(vs, 2, Some("i")))
        assert(vs.dataType == StructType(Array(
          StructField("v1", StructType(Array(
            field(0, IntegerType, "$.a"), field(1, IntegerType, "$.b")))),
          StructField("v2", StructType(Array(field(0, IntegerType, "$.a")))),
          StructField("i", IntegerType))))
      case _ => fail()
    }

    def variantGet(child: Expression): Expression = VariantGet(
      child,
      path = Literal("$.a"),
      targetType = VariantType,
      failOnError = true,
      timeZoneId = Some(localTimeZone))

    // No push down if the struct containing variant is used.
    sql("select vs, variant_get(vs.v1, '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vs = output(1)
        assert(projectList(0) == vs)
        checkAlias(projectList(1), "a", variantGet(GetStructField(vs, 0, Some("v1"))))
        assert(vs.dataType == StructType(Array(
          StructField("v1", VariantType),
          StructField("v2", VariantType),
          StructField("i", IntegerType))))
      case _ => fail()
    }

    // No push down for variant in array.
    sql("select variant_get(va[0], '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val va = output(2)
        checkAlias(projectList(0), "a", variantGet(GetArrayItem(va, Literal(0))))
        assert(va.dataType == ArrayType(VariantType))
      case _ => fail()
    }

    // No push down if variant has default value.
    sql("select variant_get(vd, '$.a') as a from T").queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val vd = output(3)
        checkAlias(projectList(0), "a", variantGet(vd))
        assert(vd.dataType == VariantType)
      case _ => fail()
    }

    // No push down if the path in variant_get is not a literal
    sql("select variant_get(v, '$.a', 'int') as a, variant_get(v, s, 'int') v2, v, " +
      "cast(v as struct<b float>) as v from T")
      .queryExecution.optimizedPlan match {
      case Project(projectList, l: LogicalRelation) =>
        val output = l.output
        val v = output(0)
        val s = output(4)
        checkAlias(projectList(0), "a", GetStructField(v, 0))
        checkAlias(projectList(1), "v2", VariantGet(GetStructField(v, 1), s,
          targetType = IntegerType, failOnError = true, timeZoneId = Some(localTimeZone)))
        checkAlias(projectList(2), "v", GetStructField(v, 1))
        checkAlias(projectList(3), "v", GetStructField(v, 2))
        assert(v.dataType == StructType(Array(
          field(0, IntegerType, "$.a"),
          field(1, VariantType, "$", timeZone = "UTC"),
          field(2, StructType(Array(StructField("b", FloatType))), "$"))))
      case _ => fail()
    }
  }

  test("No push down for JSON") {
    withTable("T") {
      sql("create table T (v variant) using JSON")
      sql("select variant_get(v, '$.a') from T").queryExecution.optimizedPlan match {
        case Project(_, l: LogicalRelation) =>
          val output = l.output
          assert(output(0).dataType == VariantType)
        case _ => fail()
      }
    }
  }
}

// V2 DataSource tests
class PushVariantIntoScanV2Suite extends PushVariantIntoScanSuiteBase {
  import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME

  override def sparkConf: SparkConf =
    super.sparkConf
      .set(s"spark.sql.catalog.$SESSION_CATALOG_NAME",
           "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog")

  private def testOnV2Formats(fn: String => Unit): Unit = {
    for (format <- Seq("PARQUET")) {
      test(s"V2 test - $format") {
        withTable("T_V2") {
          fn(format)
        }
      }
    }
  }

  testOnV2Formats { format =>
    // Create table using V2 catalog
    sql(s"create table T_V2 (v variant, vs struct<v1 variant, v2 variant, i int>, " +
        s"va array<variant>, vd variant default parse_json('1'), s string) " +
        s"using $format")

    // Test basic variant field extraction with V2
    val plan = sql(
      "select variant_get(v, '$.a', 'int') as a, v, cast(v as struct<b float>) as v from T_V2"
    ).queryExecution.optimizedPlan

    plan.foreach {
      case p =>
        // scalastyle:off println
        println(p.getClass.getSimpleName)
    }

    plan match {
      case Project(projectList, scanRelation: DataSourceV2ScanRelation) =>
        val output = scanRelation.output
        val v = output(0)
        // Check that variant pushdown happened - v should be a struct, not variant
        assert(v.dataType.isInstanceOf[StructType],
          s"Expected v to be struct type after pushdown, but got ${v.dataType}")
        val vStruct = v.dataType.asInstanceOf[StructType]
        assert(vStruct.fields.length == 3,
          s"Expected 3 fields in struct, got ${vStruct.fields.length}")
        assert(vStruct.fields(0).dataType == IntegerType)
        assert(vStruct.fields(1).dataType == VariantType)
        assert(vStruct.fields(2).dataType.isInstanceOf[StructType])
      case _ =>
        fail(s"Expected V2 scan relation with variant pushdown, got ${plan.getClass.getName}")
    }

    // Test V2 variant pushdown with filters
    sql(
      "select variant_get(v, '$.x', 'string') as x from T_V2 " +
      "where variant_get(v, '$.a', 'int') > 5"
    ).queryExecution.optimizedPlan match {
      case Project(_, Filter(_, scanRelation: DataSourceV2ScanRelation)) =>
        val output = scanRelation.output
        val v = output(0)
        assert(v.dataType.isInstanceOf[StructType],
          s"Expected v to be struct type after pushdown, but got ${v.dataType}")
        val vStruct = v.dataType.asInstanceOf[StructType]
        assert(vStruct.fields.length == 2,
          s"Expected 2 fields in struct (x and a), got ${vStruct.fields.length}")
        assert(vStruct.fields(0).dataType == StringType)
        assert(vStruct.fields(1).dataType == IntegerType)
      case _ => fail("Expected filtered V2 scan relation with variant pushdown")
    }

    // Test V2 nested struct variant pushdown
    sql("select variant_get(vs.v1, '$.nested', 'double') as nested from T_V2")
      .queryExecution.optimizedPlan match {
      case Project(_, scanRelation: DataSourceV2ScanRelation) =>
        val output = scanRelation.output
        val vs = output(1)
        val vsStruct = vs.dataType.asInstanceOf[StructType]
        val v1Type = vsStruct.fields(0).dataType.asInstanceOf[StructType]
        assert(v1Type.fields.length == 1)
        assert(v1Type.fields(0).dataType == DoubleType)
      case _ => fail("Expected V2 scan relation with nested variant pushdown")
    }

    // Test V2 multiple variant field extractions
    sql("select variant_get(v, '$.a', 'int') as a, variant_get(v, '$.b', 'string') as b, " +
        "variant_get(v, '$.c', 'boolean') as c from T_V2")
      .queryExecution.optimizedPlan match {
      case Project(_, scanRelation: DataSourceV2ScanRelation) =>
        val output = scanRelation.output
        val v = output(0)
        assert(v.dataType.isInstanceOf[StructType],
          s"Expected v to be struct type after pushdown, but got ${v.dataType}")
        val vStruct = v.dataType.asInstanceOf[StructType]
        assert(vStruct.fields.length == 3,
          s"Expected 3 fields in struct, got ${vStruct.fields.length}")
        assert(vStruct.fields(0).dataType == IntegerType)
        assert(vStruct.fields(1).dataType == StringType)
        assert(vStruct.fields(2).dataType == BooleanType)
      case _ => fail("Expected V2 scan relation with multiple variant extractions")
    }
  }

  test("V2 No push down for JSON") {
    withTable("T_V2_JSON") {
      sql("create table T_V2_JSON (v variant) using JSON")
      sql("select variant_get(v, '$.a') from T_V2_JSON").queryExecution.optimizedPlan match {
        // JSON format should not support V2 variant pushdown
        case Project(_, scanRelation: DataSourceV2ScanRelation) =>
          val output = scanRelation.output
          assert(output(0).dataType == VariantType)
        case Project(_, _: LogicalRelation) =>
          // Fallback to V1 - also acceptable
        case _ => fail("Expected scan relation without variant pushdown for JSON")
      }
    }
  }

  test("V2 variant pushdown with default values") {
    withTable("T_V2_DEFAULT") {
      sql("create table T_V2_DEFAULT (v variant default parse_json('{\"x\": 10}'), " +
          "s string) using PARQUET")

      sql("select variant_get(v, '$.y', 'int') as y from T_V2_DEFAULT")
        .queryExecution.optimizedPlan match {
        case Project(_, scanRelation: DataSourceV2ScanRelation) =>
          val output = scanRelation.output
          val v = output(0)
          // Variant with default values should NOT be pushed down (see V1 test)
          // So v should remain as VariantType
          assert(v.dataType == VariantType,
            s"Expected v to remain as VariantType (no pushdown with defaults), " +
            s"but got ${v.dataType}")
        case _ => fail("Expected V2 scan relation")
      }
    }
  }
}
