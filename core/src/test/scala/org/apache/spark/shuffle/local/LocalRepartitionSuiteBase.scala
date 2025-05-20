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

package org.apache.spark.shuffle.local

import java.util.concurrent.Executors

import org.apache.spark.SparkEnv
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD

trait LocalRepartitionSuiteBase {
  val threadExecutor = Executors.newFixedThreadPool(10)

  def getSerializedRDD(rdd: RDD[Long]): Array[Byte] = {
    rdd.isBarrier()
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    JavaUtils.bufferToArray(
      closureSerializer.serialize(rdd: AnyRef))
  }
}
