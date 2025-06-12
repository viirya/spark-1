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
package org.apache.spark.rdd

import java.io._
import java.util.Optional
import java.util.Properties
import java.util.concurrent.{Callable, Executors, ExecutorService, Future}

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, LocalRepartitionDependency, Partition, Partitioner, SparkContext, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.shuffle.local._
import org.apache.spark.util.Utils

class LocalRepartitionPartition(
    rddId: Int, val index: Int, val inputPartitions: Array[Partition]) extends Partition {
  override def hashCode(): Int = 31 * (31 + rddId) + index
  override def equals(other: Any): Boolean = super.equals(other)
}

@DeveloperApi
class LocalRepartitionRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient var rdd: RDD[T],
    val parentRDDID: Int,
    val part: Partitioner,
    val serializedRDD: Array[Byte])
  extends RDD[T](sc, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new LocalRepartitionDependency(rdd))
  }

  val queueSize = conf.get(config.LOCAL_REPARTITION_BUFFER_SIZE)

  val senderQueueSize = conf.get(config.LOCAL_REPARTITION_SENDER_BUFFER_SIZE)
  val receiverQueueSize = conf.get(config.LOCAL_REPARTITION_RECEIVER_BUFFER_SIZE)
  val maxSenderNum = conf.get(config.LOCAL_REPARTITION_SENDER_MAXNUM)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    LocalRepartition.initiate(this, split.asInstanceOf[LocalRepartitionPartition], context,
      queueSize, senderQueueSize, receiverQueueSize, maxSenderNum)

    new Iterator[T] {
      private lazy val receiver = LocalRepartition.getReceiver(id, split.index)
      private val recvFuture = receiver.recv()
      private var item: Optional[T] = null

      override def hasNext: Boolean = {
        if (!receiver.isClosed) {
          if (item == null) {
            // Initialize item on first call to hasNext
            item = recvFuture.get().asInstanceOf[Optional[T]]
          }

          val hasData = item.isPresent
          if (!hasData) {
            receiver.close()
          }
          hasData
        } else {
          false
        }
      }

      override def next(): T = {
        if (item == null) {
          hasNext
        }

        val data = item.get()
        item = null
        data
      }
    }
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  override protected def getPartitions: Array[Partition] = {
    val result = new Array[Partition](part.numPartitions)

    for (i <- 0 until part.numPartitions) {
      result(i) = new LocalRepartitionPartition(id, i, rdd.partitions)
    }

    result
  }

  @transient private var receiver: Option[Receiver[Any]] = None

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
  }
}

object LocalRepartition {
  /**
   * A thread pool for sending data to the LocalRepartitionRDD.
   */
  final val nonVirtualfactory =
    Thread.ofPlatform().name("local-repartition-sender-thread-", 0).factory
  final val factory = Thread.ofVirtual.name("local-repartition-sender-virtual-thread-", 0).factory
  final val virtualThreadexecutor = Executors.newThreadPerTaskExecutor(factory)

  /**
   * A map to store the channels for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a map from output partition index to the receiver.
   */
  private val channelMap = new mutable.HashMap[Int, mutable.ArrayBuffer[Option[Receiver[Any]]]]()

  private val tasksMap =
    new mutable.HashMap[Int, mutable.ArrayBuffer[Future[Optional[Throwable]]]]()

  /**
   * Initialize the channel map for the given LocalRepartitionRDD.
   * Launch async tasks for each input partition.
   * This method is thread-safe.
   */
  def initiate[T: ClassTag](
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      context: TaskContext,
      queueSize: Int,
      senderQueueSize: Int,
      receiverQueueSize: Int,
      maxSenderNum: Int): Unit =
    LocalRepartition.synchronized {
      channelMap.synchronized {
        // Initiate the channel map for the local repartition rdd if it doesn't exist.
        if (!channelMap.contains(rdd.id)) {
          channelMap(rdd.id) =
            new mutable.ArrayBuffer[Option[Receiver[Any]]]

          // Create a channel for each output partition
          val channels = Channel.createChannels[T](rdd.part.numPartitions, queueSize)
            .asScala.toArray

          // Create a sender for each input partition
          val senders = mutable.ArrayBuffer[Sender[Any]]()
          for (i <- 0 until split.inputPartitions.length) {
            val senderContext = createSenderTaskContext(context, i, split.inputPartitions.length)
            senders += new Sender(rdd.parentRDDID, channels, senderQueueSize, SparkEnv.get,
              senderContext).asInstanceOf[Sender[Any]]
          }

          // Create a receiver for each output partition
          for (i <- 0 until rdd.part.numPartitions) {
            channelMap(rdd.id) +=
              Some(channels(i).createReceiver(rdd.id, receiverQueueSize)
                .asInstanceOf[Receiver[Any]])
          }

          // Launch one async task per *input* partition
          launchInputTasks(senders.toSeq, rdd, split, rdd.part, maxSenderNum)
        } else {
          // no-op
        }
      }

      context.addTaskCompletionListener[Unit](_ => {
        channelMap.synchronized {
          val receiver = channelMap(rdd.id)(split.index)
          if (receiver.isDefined) {
            receiver.get.close()
            channelMap(rdd.id)(split.index) = None
          }

          if (channelMap(rdd.id).forall(_.isEmpty)) {
            channelMap.remove(rdd.id)
          }
        }

        tasksMap.synchronized {
          val tasks = tasksMap.get(rdd.id)
          if (tasks.isDefined) {
            tasks.get.foreach { task =>
              if (task != null && task.isDone) {
                val err = task.get()
                if (err.isPresent) {
                  throw err.get
                }
              }
            }
          }
        }
      })
    }

  def getReceiver(rddId: Int, partitionIndex: Int): Receiver[Any] = {
    if (!channelMap.contains(rddId)) {
      throw new IllegalStateException(s"Channel map for RDD $rddId not found.")
    }
    if (partitionIndex < 0 || partitionIndex >= channelMap(rddId).length) {
      throw new
          IllegalStateException(s"Receiver for RDD $rddId and partition $partitionIndex not found.")
    }
    if (channelMap(rddId)(partitionIndex).isEmpty) {
      throw new
          IllegalStateException(s"Receiver for RDD $rddId and partition $partitionIndex is closed.")
    }
    channelMap(rddId)(partitionIndex).get
  }

  /**
   * Launch the input tasks for the given LocalRepartitionRDD.
   */
  def launchInputTasks[T: ClassTag](
      senders: Seq[Sender[Any]],
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      part: Partitioner,
      maxSenderNum: Int): Unit = {
    val clazz = implicitly[ClassTag[RDD[T]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]

    val tasks = new mutable.ArrayBuffer[Future[Optional[Throwable]]]()
    for (i <- 0 until split.inputPartitions.length) {
      tasks += null
    }
    tasksMap.put(rdd.id, tasks)

    val callBack = new SenderCallBack(senders, maxSenderNum,
      rdd.serializedRDD, split.inputPartitions, clazz, part, virtualThreadexecutor, tasks)

    for (i <- 0 until split.inputPartitions.length) {
      if (i < maxSenderNum) {
        val task = senders(i)
          .send(rdd.serializedRDD, split.inputPartitions(i), clazz, part, callBack)
          .getFuture(virtualThreadexecutor)
        tasks(i) = task
      }
    }
  }

  /**
   * Create a new TaskContext for the sender task.
   */
  def createSenderTaskContext(context: TaskContext, splitId: Int, numSplits: Int): TaskContext = {
    val taskAttemptId = TaskSchedulerImpl.newTaskId()
    val blockManager = SparkEnv.get.blockManager
    blockManager.registerTask(taskAttemptId)

    val taskMemoryManager = new TaskMemoryManager(SparkEnv.get.memoryManager, taskAttemptId)
    new TaskContextImpl(
      context.stageId(),
      context.stageAttemptNumber(),
      splitId,
      taskAttemptId,
      0,
      numSplits,
      taskMemoryManager,
      new Properties,
      context.getMetricsSystem(),
      TaskMetrics.empty,
      context.cpus(),
      context.resources())
  }
}

class SenderCallBack(
    val senders: Seq[Sender[Any]],
    val startSenderIdx: Int,
    val serializedRDD: Array[Byte],
    val inputPartitions: Array[Partition],
    val clazz: Class[RDD[Any]],
    val part: Partitioner,
    val threadExecutor: ExecutorService,
    val tasks: mutable.ArrayBuffer[Future[Optional[Throwable]]]) extends Callable[Void] {

  val senderIdx = new java.util.concurrent.atomic.AtomicInteger(startSenderIdx)

  override def call(): Void = {
    val idx = senderIdx.getAndIncrement()

    if (idx > 0 && idx < senders.length) {
      val future = senders(idx)
        .send(serializedRDD, inputPartitions(idx), clazz, part, this)
        .getFuture(threadExecutor)
      if (tasks != null && idx < tasks.length) {
        tasks(idx) = future
      }
    }

    null
  }
}
