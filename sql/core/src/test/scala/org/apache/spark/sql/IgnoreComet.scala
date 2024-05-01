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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.test.SQLTestUtils

/**
 * Tests with this tag will be ignored when Comet is enabled (e.g., via `ENABLE_COMET`).
 */
case class IgnoreComet(reason: String) extends Tag("DisableComet")

/**
 * Helper trait that disables Comet for all tests regardless of default config values.
 */
trait IgnoreCometSuite extends SQLTestUtils {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    if (isCometEnabled) {
      ignore(testName + " (disabled when Comet is on)", testTags: _*)(testFun)
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }
}
