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

package org.apache.spark.deploy.master

import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark._
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor

/**
 * End-to-end tests for preemptative priority scheduling in standalone mode.
 */
class StandalonePreemptativeSuite 
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfterAll {

  private val numWorkers = 2
  private val conf = new SparkConf()
  private val securityManager = new SecurityManager(conf)

  private var masterRpcEnv: RpcEnv = null
  private var workerRpcEnvs: Seq[RpcEnv] = null
  private var master: Master = null
  private var workers: Seq[Worker] = null

  /**
   * Start the local cluster.
   * Note: local-cluster mode is insufficient because we want a reference to the Master.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    masterRpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityManager)
    workerRpcEnvs = (0 until numWorkers).map { i =>
      RpcEnv.create(Worker.SYSTEM_NAME + i, "localhost", 0, conf, securityManager)
    }
    master = makeMaster()
    workers = makeWorkers(10, 2048)
  }

  override def afterAll(): Unit = {
    masterRpcEnv.shutdown()
    workerRpcEnvs.foreach(_.shutdown())
    master.stop()
    workers.foreach(_.stop())
    masterRpcEnv = null
    workerRpcEnvs = null
    master = null
    workers = null
    super.afterAll()
  }

  test("basic priority scheduling") {
    sc = new SparkContext(appConf("production"))
    val appId = sc.applicationId
    assert(master.apps.size === 1)
    assert(master.apps.head.id === appId)
    // We can acquire 5 cores, spread out to 2 executors on 2 workers
    assert(master.apps.head.executors.size === 2)
    assert(master.apps.head.getExecutorLimit === Int.MaxValue)
    // kill all executors
    assert(killAllExecutors(sc))
    assert(master.apps.head.executors.size === 0)
    assert(master.apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    // Because we can have 1 executor at most, it can't spread out to 2 workers as before
    assert(master.apps.head.executors.size === 1)
    assert(master.apps.head.getExecutorLimit === 1)
    // request 1 more
    assert(sc.requestExecutors(1))
    // We still only have 1 executor becasue production pool is allowed 5 cores
    assert(master.apps.head.executors.size === 1)
    assert(master.apps.head.getExecutorLimit === 2)
    // request 1 more; this one won't go through
    assert(sc.requestExecutors(1))
    assert(master.apps.head.executors.size === 1)
    assert(master.apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 1 = 2 executors
    assert(killAllExecutors(sc))
    // Because we can have 2 executors at most now, it will be spread out to 2 workers
    assert(master.apps.head.executors.size === 2)
    assert(master.apps.head.getExecutorLimit === 2)
    // kill all executors again; this time we'll have 2 - 2 = 0 executors left
    assert(killAllExecutors(sc))
    assert(master.apps.head.executors.size === 0)
    assert(master.apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    assert(master.apps.head.executors.size === 2)
    assert(master.apps.head.getExecutorLimit === 1000)
  }

  test("preemptative priority scheduling") {
    // Schedule 10 applications with lower priority
    // Each acquires 1 core
    // Total is 10 cores
    val lowPriorityApps = (0 to 9).map { i =>
      sc = new SparkContext(appConf("test"))
      val appId = sc.applicationId
      assert(master.apps.size === i + 1)
      val app = master.apps.find(app => app.id == appId)
      assert(app != None)
      // We can acquire 1 core, spread out to 1 executor on 1 worker
      assert(app.get.executors.size === 1)
      assert(app.get.getExecutorLimit === Int.MaxValue)
      app.get
    }
    // Schedule two higher priority applications
    // Each acquires 5 cores
    // Total 10 cores
    (0 to 1).map { i =>
      sc = new SparkContext(appConf("production"))
      val appId = sc.applicationId
      assert(master.apps.size === i + 11)
      val app = master.apps.find(app => app.id == appId)
      assert(app != None)
      // We can acquire 5 cores, spread out to 2 executors on 2 workers
      assert(app.get.executors.size === 2)
      assert(app.get.getExecutorLimit === Int.MaxValue)
    }
    // Now we schedule another higher priority application
    sc = new SparkContext(appConf("production"))
    val appId = sc.applicationId
    assert(master.apps.size === 13)
    val app = master.apps.find(app => app.id == appId)
    assert(app != None)
    // The applications in production pool will acquire 5 cores
    // If there are not enough cores, it will preempt lower priority applications until
    // it has 1 core at lease (min_cores)
    assert(app.get.executors.size === 1)
    assert(app.get.getExecutorLimit === Int.MaxValue)
    // We count for the lower priority applications that have been preempted
    assert(lowPriorityApps.filter(app => app.executors.size == 0).size == 1)
  }

  // ===============================
  // | Utility methods for testing |
  // ===============================

  /** Return a SparkConf for applications that want to talk to our Master. */
  private def appConf(poolName: String): SparkConf = {
    new SparkConf()
      .setMaster(masterRpcEnv.address.toSparkURL)
      .setAppName("test")
      .set("spark.app.pool", poolName)
      .set("spark.executor.memory", "256m")
  }

  /** Make a master to which our application will send executor requests. */
  private def makeMaster(): Master = {
    val setting: SchedulingSetting = SchedulingSetting(SchedulingMode.PRIORITY, None)
    val master = new Master(masterRpcEnv, masterRpcEnv.address, 0, securityManager, conf, setting)
    masterRpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    master
  }

  /** Make a few workers that talk to our master. */
  private def makeWorkers(cores: Int, memory: Int): Seq[Worker] = {
    (0 until numWorkers).map { i =>
      val rpcEnv = workerRpcEnvs(i)
      val worker = new Worker(rpcEnv, 0, cores, memory, Array(masterRpcEnv.address),
        Worker.SYSTEM_NAME + i, Worker.ENDPOINT_NAME, null, conf, securityManager)
      rpcEnv.setupEndpoint(Worker.ENDPOINT_NAME, worker)
      worker
    }
  }

  /** Kill all executors belonging to this application. */
  private def killAllExecutors(sc: SparkContext): Boolean = {
    killNExecutors(sc, Int.MaxValue)
  }

  /** Kill N executors belonging to this application. */
  private def killNExecutors(sc: SparkContext, n: Int): Boolean = {
    syncExecutors(sc)
    sc.killExecutors(getExecutorIds(sc).take(n))
  }

  /**
   * Return a list of executor IDs belonging to this application.
   *
   * Note that we must use the executor IDs according to the Master, which has the most
   * updated view. We cannot rely on the executor IDs according to the driver because we
   * don't wait for executors to register. Otherwise the tests will take much longer to run.
   */
  private def getExecutorIds(sc: SparkContext): Seq[String] = {
    assert(master.idToApp.contains(sc.applicationId))
    master.idToApp(sc.applicationId).executors.keys.map(_.toString).toSeq
  }

  /**
   * Sync executor IDs between the driver and the Master.
   *
   * This allows us to avoid waiting for new executors to register with the driver before
   * we submit a request to kill them. This must be called before each kill request.
   */
  private def syncExecutors(sc: SparkContext): Unit = {
    val driverExecutors = sc.getExecutorStorageStatus
      .map(_.blockManagerId.executorId)
      .filter { _ != SparkContext.DRIVER_IDENTIFIER}
    val masterExecutors = getExecutorIds(sc)
    val missingExecutors = masterExecutors.toSet.diff(driverExecutors.toSet).toSeq.sorted
    missingExecutors.foreach { id =>
      // Fake an executor registration so the driver knows about us
      val port = System.currentTimeMillis % 65536
      val endpointRef = mock(classOf[RpcEndpointRef])
      val mockAddress = mock(classOf[RpcAddress])
      when(endpointRef.address).thenReturn(mockAddress)
      val message = RegisterExecutor(id, endpointRef, s"localhost:$port", 10, Map.empty)
      val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
      backend.driverEndpoint.askWithRetry[CoarseGrainedClusterMessage](message)
    }
  }

}
