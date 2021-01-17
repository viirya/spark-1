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

package org.apache.spark.sql.execution.streaming.state

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.Utils

class RocksDbStateStoreSuite
  extends StateStoreSuiteBase[RocksDbStateStoreProvider]
    with BeforeAndAfter
    with PrivateMethodTester {

  import StateStoreTestsHelper._

  override val keySchema = StructType(Seq(StructField("key", StringType, true)))
  override val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  test("New get, put, remove, commit, and all data iterator") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    assert(!store.hasCommitted)
    assert(get(store, "a") === None)
    assert(store.iterator().isEmpty)

    // Verify state after updating
    put(store, "a", 1)
    assert(get(store, "a") === Some(1))

    assert(store.iterator().nonEmpty)
    assert(getLatestData(provider).isEmpty)

    // Make updates, commit and then verify state
    put(store, "b", 2)
    put(store, "aa", 3)
    remove(store, _.startsWith("a"))
    assert(store.commit() === 1)

    assert(store.hasCommitted)
    assert(rowsToSet(store.iterator()) === Set("b" -> 2))
    assert(getLatestData(provider) === Set("b" -> 2))

    // Trying to get newer versions should fail
    intercept[Exception] {
      provider.getStore(2)
    }
    intercept[Exception] {
      getData(provider, 2)
    }

    // New updates to the reloaded store with new version, and does not change old version
    val reloadedProvider = newStoreProvider(store.id, provider.getLocalDir)
    val reloadedStore = reloadedProvider.getStore(1)
    put(reloadedStore, "c", 4)
    assert(reloadedStore.commit() === 2)
    assert(rowsToSet(reloadedStore.iterator()) === Set("b" -> 2, "c" -> 4))
    assert(getLatestData(provider) === Set("b" -> 2, "c" -> 4))
    assert(getData(provider, version = 1) === Set("b" -> 2))
  }

  test("cleaning") {
    val provider =
      newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    for (i <- 1 to 20) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    require(
      rowsToSet(provider.latestIterator()) === Set("a" -> 20),
      "store not updated correctly")

    assert(!fileExists(provider, version = 1, isSnapshot = false)) // first file should be deleted

    // last couple of versions should be retrievable
    assert(getData(provider, 20) === Set("a" -> 20))
    assert(getData(provider, 19) === Set("a" -> 19))
  }

  testQuietly("SPARK-19677: Committing a delta file atop an existing one should not fail on HDFS") {
    val conf = new Configuration()
    conf.set("fs.fake.impl", classOf[RenameLikeHDFSFileSystem].getName)
    conf.set("fs.defaultFS", "fake:///")

    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, hadoopConf = conf)
    provider.getStore(0).commit()
    provider.getStore(0).commit()

    // Verify we don't leak temp files
    val tempFiles = FileUtils
      .listFiles(new File(provider.stateStoreId.checkpointRootLocation), null, true)
      .asScala
      .filter(_.getName.startsWith("temp-"))
    assert(tempFiles.isEmpty)
  }

  test("corrupted file handling") {
    val provider =
      newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    val snapshotVersion = (0 to 10)
      .find(version => fileExists(provider, version, isSnapshot = true))
      .getOrElse(fail("snapshot file not found"))

    // Corrupt snapshot file and verify that it throws error
    provider.close()
    assert(getData(provider, snapshotVersion) === Set("a" -> snapshotVersion))
    RocksDbInstance.destroyDB(provider.rocksDbPath)

    corruptFile(provider, snapshotVersion, isSnapshot = true)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion)
    }

    // Corrupt delta file and verify that it throws error
    provider.close()
    RocksDbInstance.destroyDB(provider.rocksDbPath)
    assert(getData(provider, snapshotVersion - 1) === Set("a" -> (snapshotVersion - 1)))

    corruptFile(provider, snapshotVersion - 1, isSnapshot = false)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion - 1)
    }

    // Delete delta file and verify that it throws error
    deleteFilesEarlierThanVersion(provider, snapshotVersion)
    intercept[Exception] {
      provider.close()
      RocksDbInstance.destroyDB(provider.rocksDbPath)
      getData(provider, snapshotVersion - 1)
    }
  }

  test("not setting localDir throws error") {
    val sqlConf = new SQLConf
    val dir = newDir()
    val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
    sqlConf.setConfString(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
    val storeConf = new StateStoreConf(sqlConf)
    assert(
      storeConf.providerClass ===
        "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
    val hadoopConf = new Configuration()
    val error = intercept[IllegalArgumentException] {
      StateStore.get(storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
    }.getMessage
    assert(error.contains(s"The local directory in the executor nodes"))
  }

  test("SPARK-21145: Restarted queries create new provider instances") {
    try {
      val checkpointLocation = Utils.createTempDir().getAbsoluteFile
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      SparkSession.setActiveSession(spark)
      implicit val sqlContext = spark.sqlContext
      spark.conf.set("spark.sql.shuffle.partitions", "1")
      spark.conf.set(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key,
        "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
      spark.conf.set(RocksDbStateStoreProvider.ROCKSDB_STATE_STORE_LOCAL_DIR, newDir())
      import spark.implicits._
      val inputData = MemoryStream[Int]

      def runQueryAndGetLoadedProviders(): Seq[StateStoreProvider] = {
        val aggregated = inputData.toDF().groupBy("value").agg(count("*"))
        // stateful query
        val query = aggregated.writeStream
          .format("memory")
          .outputMode("complete")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        require(query.lastProgress != null) // at least one batch processed after start
        val loadedProvidersMethod =
          PrivateMethod[mutable.HashMap[StateStoreProviderId, StateStoreProvider]](
            'loadedProviders)
        val loadedProvidersMap = StateStore invokePrivate loadedProvidersMethod()
        val loadedProviders = loadedProvidersMap.synchronized { loadedProvidersMap.values.toSeq }
        query.stop()
        loadedProviders
      }

      val loadedProvidersAfterRun1 = runQueryAndGetLoadedProviders()
      require(loadedProvidersAfterRun1.length === 1)

      val loadedProvidersAfterRun2 = runQueryAndGetLoadedProviders()
      assert(loadedProvidersAfterRun2.length === 2) // two providers loaded for 2 runs

      // Both providers should have the same StateStoreId, but the should be different objects
      assert(
        loadedProvidersAfterRun2(0).stateStoreId === loadedProvidersAfterRun2(1).stateStoreId)
      assert(loadedProvidersAfterRun2(0) ne loadedProvidersAfterRun2(1))

    } finally {
      SparkSession.getActiveSession.foreach { spark =>
        spark.streams.active.foreach(_.stop())
        spark.stop()
      }
    }
  }

  override def newStoreProvider(): RocksDbStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  override def newStoreProvider(storeId: StateStoreId): RocksDbStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation)
  }

  def newStoreProvider(storeId: StateStoreId, localDir: String): RocksDbStateStoreProvider = {
    newStoreProvider(
      storeId.operatorId,
      storeId.partitionId,
      dir = storeId.checkpointRootLocation,
      localDir = localDir)
  }

  override def newStoreProvider(
      minDeltasForSnapshot: Int,
      numOfVersToRetainInMemory: Int): RocksDbStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0,
      minDeltasForSnapshot = minDeltasForSnapshot,
      numOfVersToRetainInMemory = numOfVersToRetainInMemory)
  }

  override def getLatestData(storeProvider: RocksDbStateStoreProvider): Set[(String, Int)] = {
    getData(storeProvider)
  }

  override def getData(
                        provider: RocksDbStateStoreProvider,
                        version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId, provider.getLocalDir)
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def newStoreProvider(
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

  def corruptFile(
                   provider: RocksDbStateStoreProvider,
                   version: Long,
                   isSnapshot: Boolean): Unit = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.delete()
    filePath.createNewFile()
  }

  override def getDefaultSQLConf(
      minDeltasForSnapshot: Int,
      numOfVersToRetainInMemory: Int): SQLConf = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf.setConf(SQLConf.STATE_STORE_COMPRESSION_CODEC, SQLConf.get.stateStoreCompressionCodec)
    sqlConf.setConfString(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider")
    sqlConf.setConfString(RocksDbStateStoreProvider.ROCKSDB_STATE_STORE_LOCAL_DIR, newDir())
    sqlConf
  }

  override def getDefaultStoreConf(): StateStoreConf = {
    val sqlConf = getDefaultSQLConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get)
    new StateStoreConf(sqlConf)
  }
}
