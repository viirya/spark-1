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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * Created by dbtsai on 4/21/17.
 *
 * Some of the APIs will be public in 2.2, so we can remove those utility once we migrate to 2.2
 */
object WithExpr {
  def apply(expr: Expression): Column = Column(expr)
}

object WithColumn {
  def apply(col: Column): Expression = col.expr
}

/**
 * Utility to flatten a Column containing struct into Seq[Column]. If the Column is not struct,
 * it will throw `IllegalArgumentException`
 */
object FlattenStructColumn {
  def apply(col: Column): Seq[Column] = {
    col.expr.dataType match {
      case struct: StructType =>
        struct.fields.map(_.name).map(name => col.getField(name) as name)
      case _ =>
        throw new IllegalArgumentException(s"Can not flatten $col since it's not a StructType.")
    }
  }
}

/**
 * Utility to get the column name from a Column object
 */
object GetColumnName {
  def apply(col: Column): String = {
    col.named.name
  }
}
