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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Benchmark to measure performance for aggregate primitives.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/RocksDbStateStoreBenchmark-results.txt".
 * }}}
 */
object RocksDbStateStoreBenchmark extends SqlBasedBenchmark {
  import StateStoreTestsHelper._

  val keySchema = StructType(Seq(StructField("key", StringType, true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  private def newStoreProviderQubole(): RocksDbStateStoreProvider = {
    newStoreProviderQubole(opId = Random.nextInt(), partition = 0)
  }

  private def newStoreProviderChermenin(): ChermeninRocksDbStateStoreProvider = {
    newStoreProviderChermenin(opId = Random.nextInt(), partition = 0)
  }

  private def newStoreProviderQubole(
    opId: Long,
    partition: Int,
    dir: String = newDir(),
    localDir: String = newDir(),
    minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
    numOfVersToRetainInMemory: Int = SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get,
    hadoopConf: Configuration = new Configuration): RocksDbStateStoreProvider = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConfString(RocksDbStateStoreProvider.ROCKSDB_STATE_STORE_LOCAL_DIR, localDir)
    val provider = new RocksDbStateStoreProvider
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      keyIndexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  private def newStoreProviderChermenin(
      opId: Long,
      partition: Int,
      dir: String = newDir(),
      minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      numOfVersToRetainInMemory: Int = SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get,
      hadoopConf: Configuration = new Configuration): ChermeninRocksDbStateStoreProvider = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    val provider = new ChermeninRocksDbStateStoreProvider
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      indexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("put") {
      StateStore.stop()
      require(!StateStore.isMaintenanceRunning)

      val N = 20 << 15

      val benchmark = new Benchmark("Put key value pairs", N, output = output)

      benchmark.addCase("Chermenin", numIters = 3) { _ =>
        val provider = newStoreProviderChermenin()

        val random = new scala.util.Random(31)

        val store = provider.getStore(0)

        for (_ <- 0 to N) {
          val key = random.nextString(50)
          val value = random.nextInt(Int.MaxValue)

          put(store, key, value)
          assert(get(store, key).get == value)
        }

        val newVersion = store.commit()
        val newStore = provider.getStore(newVersion)

        for (_ <- 0 to N) {
          val key = random.nextString(50)
          val value = random.nextInt(Int.MaxValue)

          put(newStore, key, value)
          assert(get(newStore, key).get == value)
        }
        newStore.commit()
      }

      benchmark.addCase("Qubole", numIters = 3) { _ =>
        val provider = newStoreProviderQubole()

        val random = new Random(31)

        val store = provider.getStore(0)

        for (_ <- 0 to N) {
          val key = random.nextString(50)
          val value = random.nextInt(Int.MaxValue)

          put(store, key, value)
          assert(get(store, key).get == value)
        }

        val newVersion = store.commit()
        val newStore = provider.getStore(newVersion)

        for (_ <- 0 to N) {
          val key = random.nextString(50)
          val value = random.nextInt(Int.MaxValue)

          put(newStore, key, value)
          assert(get(newStore, key).get == value)
        }
        newStore.commit()
      }

      benchmark.run()
    }
  }
}

object StateStoreTestsHelper {

  val strProj = UnsafeProjection.create(Array[DataType](StringType))
  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))

  def stringToRow(s: String): UnsafeRow = {
    strProj.apply(new GenericInternalRow(Array[Any](UTF8String.fromString(s)))).copy()
  }

  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToString(row: UnsafeRow): String = {
    row.getUTF8String(0).toString
  }

  def rowToInt(row: UnsafeRow): Int = {
    row.getInt(0)
  }

  def rowsToStringInt(row: UnsafeRowPair): (String, Int) = {
    (rowToString(row.key), rowToInt(row.value))
  }

  def rowsToSet(iterator: Iterator[UnsafeRowPair]): Set[(String, Int)] = {
    iterator.map(rowsToStringInt).toSet
  }

  def remove(store: StateStore, condition: String => Boolean): Unit = {
    store.getRange(None, None).foreach { rowPair =>
      if (condition(rowToString(rowPair.key))) store.remove(rowPair.key)
    }
  }

  def put(store: StateStore, key: String, value: Int): Unit = {
    store.put(stringToRow(key), intToRow(value))
  }

  def get(store: ReadStateStore, key: String): Option[Int] = {
    Option(store.get(stringToRow(key))).map(rowToInt)
  }

  def newDir(): String = Utils.createTempDir().toString
}
