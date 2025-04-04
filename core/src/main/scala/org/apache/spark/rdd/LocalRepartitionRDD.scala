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

import java.util.Optional
import java.util.concurrent.{CompletableFuture, Executors}

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, LocalRepartitionDependency, Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config
import org.apache.spark.shuffle.local._

class LocalRepartitionPartition(
    rddId: Int, val index: Int, val inputPartitions: Array[Partition]) extends Partition {
  override def hashCode(): Int = 31 * (31 + rddId) + index
  override def equals(other: Any): Boolean = super.equals(other)
}

@DeveloperApi
class LocalRepartitionRDD[T: ClassTag](
    sc: SparkContext,
    var rdd: RDD[T],
    val part: Partitioner)
  extends RDD[T](sc, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new LocalRepartitionDependency(rdd))
  }

  val queueSize = conf.get(config.LOCAL_REPARTITION_BUFFER_SIZE)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    LocalRepartition.initiate(this, split.asInstanceOf[LocalRepartitionPartition], context,
      queueSize)

    new Iterator[T] {
      private val receiver = LocalRepartition.getReceiver(id, split.index)
      private var recvFuture = receiver.recv()
      private var item: Optional[T] = Optional.empty()

      override def hasNext: Boolean = {
        if (!receiver.isClosed) {
          item = recvFuture.get().asInstanceOf[Optional[T]]
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
        recvFuture = receiver.recv()
        this.item.get()
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
}

object LocalRepartition {
  /**
   * A thread pool for sending data to the LocalRepartitionRDD.
   * This is a fixed thread pool with a maximum of 10 threads.
   * TODO: make the number of thread configurable?
   */
  val senderThreadExecutor = Executors.newFixedThreadPool(10)

  /**
   * A map to store the channels for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a map from output partition index to the receiver.
   */
  private val channelMap = new mutable.HashMap[Int, mutable.HashMap[Int, Receiver[Any]]]()

  /**
   * Initialize the channel map for the given LocalRepartitionRDD.
   * Launch async tasks for each input partition.
   * This method is thread-safe.
   */
  def initiate[T](
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      context: TaskContext,
      queueSize: Int): Unit =
    LocalRepartition.synchronized {
      channelMap.synchronized {
        // Initiate the channel map for the local repartition rdd if it doesn't exist.
        if (!channelMap.contains(rdd.id)) {
          channelMap(rdd.id) =
            new mutable.HashMap[Int, Receiver[Any]]()

          // Create a channel for each output partition
          val channels = Channel.createChannels[T](rdd.part.numPartitions, queueSize).asScala

          // Create a sender for each input partition
          val senders = mutable.ArrayBuffer[Sender[Any]]()
          for (_ <- 0 until split.inputPartitions.length) {
            senders += new Sender(channels.toArray).asInstanceOf[Sender[Any]]
          }

          // Create a receiver for each output partition
          for (i <- 0 until rdd.part.numPartitions) {
            channelMap(rdd.id).put(i, channels(i).createReceiver().asInstanceOf[Receiver[Any]])
          }

          // Launch one async task per *input* partition
          launchInputTasks(senders.toSeq, rdd, split, rdd.part, context)
        } else {
          // no-op
        }
      }

      context.addTaskCompletionListener((_) => {
        channelMap.synchronized {
          LocalRepartition.channelMap(rdd.id).remove(split.index)
          if (LocalRepartition.channelMap(rdd.id).isEmpty) {
            LocalRepartition.channelMap.remove(rdd.id)
          }
        }
      })
    }

  def getReceiver(rddId: Int, partitionIndex: Int): Receiver[Any] = {
    channelMap(rddId)(partitionIndex)
  }

  /**
   * Launch the input tasks for the given LocalRepartitionRDD.
   */
  def launchInputTasks[T](
      senders: Seq[Sender[Any]],
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      part: Partitioner,
      context: TaskContext): Unit = {
    val tasks = new mutable.ArrayBuffer[CompletableFuture[Void]]()
    for (i <- 0 until split.inputPartitions.length) {
      val inputIterator = rdd.rdd.iterator(split.inputPartitions(i), context)

      // TODO: error handling
      tasks += senders(i).send(inputIterator, part).getFuture(senderThreadExecutor)
    }

    // All sender tasks are completed.
    CompletableFuture.allOf(tasks.toArray: _*).whenComplete((_, _) => {
      // error handling?
    })

  }
}
