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

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.{Arrays, Comparator, Date, PriorityQueue}

import scala.collection.mutable
import scala.io.Source
import scala.xml.{Elem, XML}

import org.apache.spark.{Logging, SparkException}

/**
 * An interface for sort algorithm
 */
private[master] trait SchedulingAlgorithm {
  def master: Master

  /**
   * Schedule and launch executors on workers
   */
  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit

  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int]

  def registerApplication(app: ApplicationInfo): Unit = {}

  def removeApplication(app: ApplicationInfo): Unit = {}

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign)
      master.launchExecutor(worker, exec)
    }
    app.state = ApplicationState.RUNNING
    app.startTiming
  }
}

/**
 * FIFO algorithm between Spark applications
 */
private[master] class FIFOSchedulingAlgorithm(val master: Master) extends SchedulingAlgorithm {
  /**
   * This is a very simple FIFO scheduler. We keep trying to fit in the first app
   * in the queue, then the second app, etc.
   */
  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps if app.coresLeft > 0) {
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
      // Filter out workers that don't have enough resources to launch an executor
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, master.spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }
}

case class ApplicationSubmission(val appInfo: ApplicationInfo, val submittedTime: Date)

private[master] class PrioritySchedulingAlgorithm(
    val master: Master,
    val schedulingSetting: SchedulingSetting) extends SchedulingAlgorithm with Logging {
  // XML tags and attributes for pool definition
  val DEFAULT_PRIORITY: Double = 1.0
  val DEFAULT_CORES: Int = 1
  val DEFAULT_MINCORES: Int = 0
  val POOLS_PROPERTY = "pool"
  val POOL_NAME_PROPERTY = "@name"
  val PRIORITY_PROPERTY = "priority"
  val CORES_PROPERTY = "cores"
  val MINCORES_PROPERTY = "min_cores"

  // XML tags and attributes for cluster configuration
  val CONFIG_PROPERTY = "config"
  val CONFIG_NAME_PROPERTY = "@name"
  val WORKERS_PROPERTY = "workers"
  val WORKER_NUMBER_PROPERTY = "worker_number"

  // How many workers we can allocate executors
  // By default, it is negative, we will allocate all workers
  var availableWorkers = -1

  val initAppNumberPerPool = 100
  val initPoolNumber = 5

  private final val applicationComparator: Comparator[ApplicationSubmission] =
    new Comparator[ApplicationSubmission]() {
      override def compare(left: ApplicationSubmission, right: ApplicationSubmission): Int = {
        left.submittedTime.compareTo(right.submittedTime)
      }
    }

  private final val poolComparator: Comparator[Pool] = new Comparator[Pool]() {
    override def compare(left: Pool, right: Pool): Int = {
      -Ordering[Double].compare(left.priority, right.priority)
    }
  }

  class Pool(var poolName: String, var priority: Double, var cores: Int, var min_cores: Int) {
    private val appQueue: PriorityQueue[ApplicationSubmission] =
      new PriorityQueue[ApplicationSubmission](initAppNumberPerPool, applicationComparator)

    private var deprecated: Boolean = false

    def markedDeprecated: Unit = {
      deprecated = true
    }

    def isDeprecated: Boolean = deprecated

    def addApplication(app: ApplicationSubmission): Unit = {
      appQueue.add(app)
    }

    def addApplication(app: ApplicationInfo): Unit = {
      val submission = ApplicationSubmission(app, new Date())
      addApplication(submission)
    }

    def nextApplication(): Option[ApplicationInfo] = {
      val nextOne = appQueue.poll()
      if (nextOne == null) {
        None
      } else {
        Some(nextOne.appInfo)
      }
    }

    def removeApplication(app: ApplicationInfo): Boolean = {
      val submitted = appQueue.toArray().find(_.asInstanceOf[ApplicationSubmission].appInfo == app)
      if (submitted != None) {
        appQueue.remove(submitted.get)
      } else {
        false
      }
    }

    def getApplications(): Seq[ApplicationInfo] = {
      appQueue.toArray().map(_.asInstanceOf[ApplicationSubmission].appInfo)
        .map(_.asInstanceOf[ApplicationInfo])
    }

    def size: Int = appQueue.size()
  }

  private final val poolQueue: PriorityQueue[Pool] =
    new PriorityQueue[Pool](initPoolNumber, poolComparator)

  private var queueIndex: Int = 0

  private var currentPool: Pool = _

  def queueSize(): Int = poolQueue.size()

  def poolsToArray(): Array[Pool] = {
    val pools = poolQueue.toArray().map(_.asInstanceOf[Pool])
    Arrays.sort(pools, poolComparator)
    pools
  }

  def firstPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      Some(poolQueue.peek())
    }
  }

  def nextPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      queueIndex %= queueSize()
      val nextOne = poolsToArray()(queueIndex)
      queueIndex += 1
      Some(nextOne.asInstanceOf[Pool])
    }
  }

  def nonEmptyPools(): Seq[Pool] = {
    val pools = poolsToArray()
    pools.filter(_.asInstanceOf[Pool].size > 0).map(_.asInstanceOf[Pool])
  }

  def nextNonEmptyPool(): Option[Pool] = {
    if (queueSize() == 0) {
      None
    } else {
      val pools = poolsToArray()
      pools.find(_.asInstanceOf[Pool].size > 0).map(_.asInstanceOf[Pool])
    }
  }

  private def findPool(poolName: String): Option[Pool] = {
    val pool = poolsToArray().find { p =>
      p.asInstanceOf[Pool].poolName == poolName
    }.getOrElse(null).asInstanceOf[Pool]
    if (pool == null) {
      logInfo(s"Can't find the pool: $poolName")
      None
    } else {
      logInfo(s"Found the pool: $poolName")
      Some(pool)
    }
  }

  private def removePool(poolName: String): Boolean = {
    val poolToRemove = findPool(poolName)
    if (poolToRemove.isDefined) {
      if (poolToRemove.get.size == 0) {
        logInfo(s"Trying to remove the pool: $poolName")
        val ret = poolQueue.remove(poolToRemove.get)
        if (ret) {
          logInfo(s"Successfully remove the pool: $poolName")
        } else {
          logInfo(s"Can't remove the pool: $poolName")
        }
        ret
      } else {
        logInfo(s"Can't remove the pool: $poolName, because ${poolToRemove.get.size} " +
          "applications are queued in it")
        logInfo(s"Marked it as deprecated and applications can't be queued into further")
        poolToRemove.get.markedDeprecated
        true
      }
    } else {
      false
    }
  }

  private def deprecatePools(poolNames: mutable.ArrayBuffer[String]): Unit = {
    val poolsToDeprecated: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    poolsToArray().map { p =>
      if (!poolNames.contains(p.asInstanceOf[Pool].poolName)) {
        poolsToDeprecated += p.asInstanceOf[Pool].poolName
      }
    }
    poolsToDeprecated.foreach(p => removePool(p))
  }

  override def registerApplication(app: ApplicationInfo): Unit = {
    // Refresh pool definitions
    buildPools()

    var pool: Pool = null
    if (app.desc.assignedPool == None) {
      logInfo(s"Application ${app.desc.name} hasn't been assigned to any pool")
      logInfo(s"Assign it to lowest-prioroty pool")
    } else {
      pool = poolsToArray().find { p =>
        p.asInstanceOf[Pool].poolName == app.desc.assignedPool.get
      }.getOrElse {
          // If the application is assigned to an unknown pool, we directly assign it to
          // lowest-prioroty pool
          logInfo(s"Application ${app.desc.name} has been assigned to " +
            s"unknown pool ${app.desc.assignedPool.get}")
          logInfo(s"Assign it to lowest-prioroty pool")
          null
      }.asInstanceOf[Pool]
    }
    if (pool == null) {
      pool = poolsToArray()(queueSize() - 1).asInstanceOf[Pool]
    }
    if (!pool.isDeprecated) {
      pool.addApplication(app)
      logInfo(s"Application ${app.desc.name} has assigned to the pool ${pool.poolName}")
    } else {
      logInfo(s"The pool ${pool.poolName} has been deprecated")
      logInfo(s"Assign application ${app.desc.name} to the lowest-prioroty pool")

      pool = poolsToArray()(queueSize() - 1).asInstanceOf[Pool]
      pool.addApplication(app)
      // Re-assign application to the pool
      app.desc.assignedPool = Some(pool.poolName)
      logInfo(s"Application ${app.desc.name} has assigned to the pool ${pool.poolName}")
    }
  }

  override def removeApplication(app: ApplicationInfo): Unit = {
    var pool: Pool = null
    if (app.desc.assignedPool == None) {
      logInfo(s"Application ${app.desc.name} hasn't been assigned to any pool")
      logInfo(s"Looking it up in lowest-prioroty pool")
    } else {
      pool = poolsToArray().find { p =>
        p.asInstanceOf[Pool].poolName == app.desc.assignedPool.get
      }.getOrElse {
          logInfo(s"Application ${app.desc.name} has been assigned to " +
            s"unknown pool ${app.desc.assignedPool.get}")
          logInfo(s"Looking it up in lowest-prioroty pool")
          null
      }.asInstanceOf[Pool]
    }
    if (pool == null) {
      pool = poolsToArray()(queueSize() - 1).asInstanceOf[Pool]
    }
    if (!pool.removeApplication(app)) {
      throw new SparkException(s"Can't remove application ${app.desc.name} " +
        s"from pool ${app.desc.assignedPool.get}")
    }
  }

  def startExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit = {
    // Refresh pool definitions
    buildPools()

    if (availableWorkers >= 0) {
      val workersToAllocate = workers.slice(0, availableWorkers)
      internalStartExecutorsOnWorkers(waitingApps, workersToAllocate)
    } else {
      val disallowedWorkers: Set[String] = loadDisallowedWorkers()
      val workersToAllocate = workers.filterNot(w => disallowedWorkers.contains(w.host))
      internalStartExecutorsOnWorkers(waitingApps, workersToAllocate)
    }
  }

  private def internalStartExecutorsOnWorkers(
      waitingApps: Array[ApplicationInfo],
      workers: Array[WorkerInfo]): Unit = {
    nonEmptyPools().map { pool => pool.getApplications().map { app =>
      // The allowed cores setting overwrites app.requestedCores, so we check it here
      if (pool.cores - app.coresGranted > 0) {
        logInfo(s"Application ${app.desc.name} has not granted enough cores. " +
          s"It still needs ${pool.cores - app.coresGranted} cores.")
        val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
        // Filter out workers that don't have enough resources to launch an executor
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
            worker.coresFree >= coresPerExecutor.getOrElse(1))
          .sortBy(_.coresFree).reverse
        val assignedCores =
          scheduleExecutorsOnWorkersForPool(app, usableWorkers, master.spreadOutApps, pool)

        // Whether we need to preempt the executors of other applications
        // If the already granted cores plus assigned cores in this iteration is less than
        // the cores of the pool, we will try to preempt other applications to get more cores
        val totalAssignedCores = assignedCores.sum
        if (totalAssignedCores + app.coresGranted < pool.cores) {
          val assignedCoresForAllWorkers = tryPreemptExistingExecutors(app, assignedCores, workers,
            usableWorkers, master.spreadOutApps, pool)
          for (pos <- 0 until workers.length if assignedCoresForAllWorkers(pos) > 0) {
            allocateWorkerResourceToExecutors(
              app, assignedCoresForAllWorkers(pos), coresPerExecutor, workers(pos))
          }
        } else {
          // Now that we've decided how many cores to allocate on each worker, let's allocate them
          for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
            allocateWorkerResourceToExecutors(
              app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
          }
        }
      }
    }}
  }

  private[master] def getWorkerIndex(workerInfo: WorkerInfo, workers: Array[WorkerInfo]): Int = {
    var i = 0
    workers.find { w =>
      if (w == workerInfo) {
        true
      } else {
        i += 1
        false
      }
    }
    if (i == workers.size) {
      i = -1
    }
    i
  }

  private[master] def mapAlreadyAssignedCoresToNewList(
      assignedCores: Array[Int],
      usableWorkers: Array[WorkerInfo],
      workers: Array[WorkerInfo],
      minCoresPerExecutor: Int,
      oneExecutorPerWorker: Boolean): (Array[Int], Array[Int]) = {
    val numForAllWorkers = workers.length
    val assignedCoresForAllWorkers = new Array[Int](numForAllWorkers)
    val assignedExecutorsForAllWorkers = new Array[Int](numForAllWorkers)
    for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
      val workerInfo = usableWorkers(pos)
      val i = getWorkerIndex(workerInfo, workers)
      if (i >= 0) {
        assignedCoresForAllWorkers(i) = assignedCores(pos)
      }
    }

    for (pos <- 0 until assignedCoresForAllWorkers.length) {
      if (oneExecutorPerWorker) {
        assignedExecutorsForAllWorkers(pos) = 1
      } else {
        assignedExecutorsForAllWorkers(pos) = assignedCoresForAllWorkers(pos) / minCoresPerExecutor
      }
    }

    (assignedCoresForAllWorkers, assignedExecutorsForAllWorkers)
  }

  private[master] def preemptExistingExecutor(
      appInfo: ApplicationInfo,
      desc: ExecutorDesc): Unit = {
    appInfo.removeExecutor(desc)
    master.killExecutor(desc)
  }

  private[master] def tryPreemptApplications(
      numForAllWorkers: Int,
      workers: Array[WorkerInfo],
      pool: Pool): (Array[Int], Array[Int]) = {
    // Allocate arrays for storing preempted cores and memory
    val preemptedCores = new Array[Int](numForAllWorkers)
    val preemptedMemory = new Array[Int](numForAllWorkers)

    // Filter out the pools with lower priorities
    nonEmptyPools().filter(_.priority.toInt < pool.priority.toInt).map { lowerPool =>
      // We need to reverse the applications in pool because we want to preempt
      // later submitted applications first
      lowerPool.getApplications().reverse.map { app =>
        // If this application has existing executors
        if (app.executors.values.size > 0) {
          // Read necessary information of preempted application
          val memoryPerExecutorMBForPreempted = app.desc.memoryPerExecutorMB

          app.executors.values.foreach { executor =>
            val workerInfo = executor.worker
            val pos = getWorkerIndex(workerInfo, workers)

            // Record how many cores and memory can get if the executor is preempted
            if (pos >= 0) {
              preemptedCores(pos) += executor.cores
              preemptedMemory(pos) += memoryPerExecutorMBForPreempted
            }
          }
        }
      }
    }
    (preemptedCores, preemptedMemory)
  }

  private[master] def tryPreemptExistingExecutors(
      app: ApplicationInfo,
      oldAssignedCores: Array[Int],
      workers: Array[WorkerInfo],
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean,
      pool: Pool): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    // We need to scan all workers
    val numForAllWorkers = workers.length
    val (assignedCores, assignedExecutors) = mapAlreadyAssignedCoresToNewList(oldAssignedCores,
      usableWorkers, workers, minCoresPerExecutor, oneExecutorPerWorker)
    // Calculate cores and memory obtained by preempting other applications
    val (preemptedCores, preemptedMemory) = tryPreemptApplications(numForAllWorkers, workers, pool)

    // If there are remaining cores we can obtain after preempt executors on this worker
    // We allocate them to this application
    val freeCoresOnWorkers = (0 until numForAllWorkers)
      .map(workers(_).coresFree).sum

    // We can only assign all cores of the cluster at most
    var coresToAssign = math.min(pool.cores - app.coresGranted - oldAssignedCores.sum,
      workers.map(_.cores).sum)

    // Minimal cores we want to assign for the application in this pool
    // Becasue min_cores could be less than coresGranted,
    // `minCoresToAssign` could be negative number, if so, we will simply reture already
    // assigned cores because it already satisfies minimal core requirement
    var minCoresToAssign = math.min(pool.min_cores - app.coresGranted - oldAssignedCores.sum,
      workers.map(_.cores).sum)

    // We only count for the cores on which workers we can get enough executor memory
    // That is because if there are not enough memory for our executor, the executor we
    // run on the worker will not be useful for our job
    val canAssignedCores = (0 until numForAllWorkers)
      .filter(pos => preemptedMemory(pos) + workers(pos).memoryFree >= memoryPerExecutor)
      .map(pos => preemptedCores(pos) + workers(pos).coresFree - assignedCores(pos)).sum

    logInfo(s"canAssignedCores: $canAssignedCores, coresToAssign: $coresToAssign, " +
      s"alreadyAssignedCores: ${oldAssignedCores.sum}, pool.cores: ${pool.cores}, " +
      s"pool.min_cores: ${pool.min_cores}, app.coresGranted: ${app.coresGranted}, " +
      s"totalCores: ${workers.map(_.cores).sum}")

    // Can we satisfy the core requirement and minimal core requirement
    val canSatisfyCoreReq = canAssignedCores >= coresToAssign
    val canSatisfyMinimalCoreReq = canAssignedCores >= minCoresToAssign

    // If we can't allocate any cores by preempting other applications,
    // but already assigned cores are more than `min_cores` (i.e. `minCoresToAssign` is negative),
    // we just simply return already assigned cores
    if (canAssignedCores == 0 && canSatisfyMinimalCoreReq) {
      logInfo(s"Can't allocate more cores by preempting other applications, " +
        s"but already assigned cores: ${oldAssignedCores.sum} satisfy " +
        "minimal core requirement, so simply return already assigned cores")
      return assignedCores
    }

    if (canSatisfyCoreReq || canSatisfyMinimalCoreReq) {
      // If we can only satisfy minimal core requirement, we need to reset the coresToAssign,
      // as we can only provide `canAssignedCores` cores
      if (!canSatisfyCoreReq && canSatisfyMinimalCoreReq) {
        coresToAssign = canAssignedCores
      }

      // Filter out the pools with lower priorities
      nonEmptyPools().filter(_.priority.toInt < pool.priority.toInt).map { lowerPool =>
        // We need to reverse the applications in pool because we want to preempt
        // later submitted applications first
        lowerPool.getApplications().reverse.map { app =>
          // If this application has existing executors
          if (app.executors.values.size > 0) {
            // Read necessary information of preempted application
            val memoryPerExecutorMBForPreempted = app.desc.memoryPerExecutorMB
            val coresPerExecutorForPreempted = app.desc.coresPerExecutor
            val minCoresPerExecutorForPreempted = coresPerExecutorForPreempted.getOrElse(1)

            // If we have preempted one of the executors of this application
            // We will preempt all of them to prevent it failed
            var havePreempted: Boolean = false
            app.executors.values.foreach { executor =>
              val workerInfo = executor.worker
              val pos = getWorkerIndex(workerInfo, workers)

              val keepPreempting = coresToAssign >= minCoresPerExecutor

              // If we can get enough memory on this worker
              if (pos >= 0 && preemptedMemory(pos) + workers(pos).memoryFree >= memoryPerExecutor &&
                keepPreempting) {
                havePreempted = true
              }
            }

            if (havePreempted) {
              app.executors.values.foreach { executor =>
                val workerInfo = executor.worker
                val pos = getWorkerIndex(workerInfo, workers)

                val keepPreempting = coresToAssign >= minCoresPerExecutor

                // If we can get enough memory on this worker
                if (pos >= 0 &&
                  preemptedMemory(pos) + workers(pos).memoryFree >= memoryPerExecutor &&
                  keepPreempting) {
                  preemptExistingExecutor(app, executor)

                  assignedCores(pos) += math.min(executor.cores, coresToAssign)
                  coresToAssign -= math.min(executor.cores, coresToAssign)

                  // We also allocate free cores on the worker
                  val coreRemaining = math.min(workers(pos).coresFree - assignedCores(pos),
                    coresToAssign)
                  if (coreRemaining > 0) {
                    coresToAssign -= coreRemaining
                    assignedCores(pos) += coreRemaining
                  }

                  if (oneExecutorPerWorker) {
                    assignedExecutors(pos) = 1
                  } else {
                    // The number of executors we can allocate at most on this worker
                    // It can't be more than available memory
                    assignedExecutors(pos) = math.min(assignedCores(pos) /  minCoresPerExecutor,
                      (preemptedMemory(pos) + workers(pos).memoryFree) / memoryPerExecutor)
                  }
                } else {
                  // If this worker has no enough memory or
                  // we already have enough cores for our application,
                  // we still preempt the remaining executors of this application.
                  // But we don't allocate these cores to our application
                  preemptExistingExecutor(app, executor)
                }
              }
              // Reset application state to waiting
              app.state = ApplicationState.WAITING
              // Stop to count for running time
              app.stopTiming
            }
          }
        }
      }
      logInfo(s"After preempting other applications, this application can be allocated: " +
        s"${assignedCores.sum} cores")
      assignedCores
    } else {
      logInfo(s"Can not satisfy the minimal core requirement: ${pool.min_cores} cores, " +
        "skip preempting this time")
      new Array[Int](numForAllWorkers)
    }
  }

  /**
   * A wrapper for scheduleExecutorsOnWorkers method with additional pool parameter
   */
  def scheduleExecutorsOnWorkersForPool(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean,
      pool: Pool): Array[Int] = {
    currentPool = pool
    scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
  }

  def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(currentPool.cores - app.coresGranted,
      usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }

  def loadDefault(): InputStream = {
    val exampleXML = """<?xml version="1.0"?>
                        <allocations>
                          <config name="workers">
                            <worker_number>10</worker_number>
                          </config>
                          <pool name="production">
                            <priority>10</priority>
                            <cores>5</cores>
                            <min_cores>1</min_cores>
                          </pool>
                          <pool name="test">
                            <priority>2</priority>
                            <cores>1</cores>
                            <min_cores>0</min_cores>
                          </pool>
                        </allocations>"""
    new ByteArrayInputStream(exampleXML.getBytes(StandardCharsets.UTF_8))
  }

  def loadDisallowedWorkers(): Set[String] = {
    var is: Option[InputStream] = None
    val workers: mutable.ArrayBuffer[String] = mutable.ArrayBuffer[String]()
    try {
      is = Option {
        schedulingSetting.disabledWorkerFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          logInfo("No disabled worker file is specified for priority scheduling")
          null
        }
      }
      is.foreach { i =>
        val buffered = Source.fromInputStream(i).getLines
        buffered.copyToBuffer(workers)
      }
    } finally {
      is.foreach(_.close())
    }
    workers.toSet
  }

  def buildPools(): PrioritySchedulingAlgorithm = {
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulingSetting.configFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          logError("Must specify configuration file for priority scheduling")
          loadDefault()
        }
      }
      is.foreach { i =>
        val xml = XML.load(i)
        buildFairSchedulerPool(xml)
        setClusterConfig(xml)
      }
    } finally {
      is.foreach(_.close())
    }
    this
  }

  def setClusterConfig(xml: Elem): Unit = {
    for (configNode <- (xml \\ CONFIG_PROPERTY)) {
      val configName = (configNode \ CONFIG_NAME_PROPERTY).text

      configName match {
        case WORKERS_PROPERTY =>
          val xmlNumber = (configNode \ WORKER_NUMBER_PROPERTY).text
          if (xmlNumber != "") {
            availableWorkers = xmlNumber.toInt
          }
          logInfo(s"workers configuration: $availableWorkers")
        case _ =>
          logInfo(s"Unknown configuration: $configName")
      }
    }
  }

  def buildFairSchedulerPool(xml: Elem): Unit = {
    val importedPools = mutable.ArrayBuffer[String]()
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      var priority: Double = DEFAULT_PRIORITY
      var cores: Int = DEFAULT_CORES
      var min_cores: Int = DEFAULT_MINCORES

      val xmlPriority = (poolNode \ PRIORITY_PROPERTY).text
      if (xmlPriority != "") {
        priority = xmlPriority.toDouble
      }

      val xmlCores = (poolNode \ CORES_PROPERTY).text
      if (xmlCores != "") {
        cores = xmlCores.toInt
      }

      val xmlMinCores = (poolNode \ MINCORES_PROPERTY).text
      if (xmlMinCores != "") {
        min_cores = xmlMinCores.toInt
      }

      val pool = new Pool(poolName, priority, cores, min_cores)
      val existingPool = findPool(poolName)
      if (existingPool.isDefined) {
        existingPool.get.poolName = poolName
        existingPool.get.priority = priority
        existingPool.get.cores = cores
        existingPool.get.min_cores = min_cores
      } else {
        poolQueue.add(pool)
      }
      importedPools += poolName
      logInfo("Created pool %s, priority: %f, cores: %d, min_cores: %d".format(
        poolName, priority, cores, min_cores))
    }
    deprecatePools(importedPools)
  }
}
