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

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    LocalRepartition.initiate(this, split.asInstanceOf[LocalRepartitionPartition], context)

    new Iterator[T] {
      private val receiver = LocalRepartition.getReceiver(id, split.index)
      private var recvFuture = receiver.recv()
      private var item: Optional[T] = Optional.empty()

      override def hasNext: Boolean = {
        if (!receiver.isClosed) {
          item = recvFuture.getFuture().asInstanceOf[Optional[T]]
          item.isPresent
        } else {
          receiver.close()
          false
        }
      }

      override def next(): T = {
        recvFuture = receiver.recv()
        // scalastyle:off println
        // println(s"got item: $item")
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
  val senderThreadExecutor = Executors.newFixedThreadPool(10)
  val receiverThreadExecutor = Executors.newFixedThreadPool(10)

  /**
   * A map to store the channels for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a map from output partition index to a pair of
   * (senders, receiver).
   */
  private val channelMap = new mutable.HashMap[Int, mutable.HashMap[Int, Receiver[Any]]]()

  /**
   * A map to store the async tasks for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a sequence of async tasks, one per input partition.
   */
  private val sawnedTasks = mutable.HashMap[Int, CompletableFuture[Void]]()

  /**
   * Initialize the channel map for the given LocalRepartitionRDD.
   * This method is thread-safe.
   * @param rdd
   * @tparam T
   */
  def initiate[T](
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      context: TaskContext): Unit =
    LocalRepartition.synchronized {
      // scalastyle:off println
      // println(s"initiate channel map for rdd: ${rdd.id}, " +
      //  s"input partition size: ${split.inputPartitions.size}")

      channelMap.synchronized {
        if (!channelMap.contains(rdd.id)) {
          // scalastyle:off println
          // println(s"no channel map for ${rdd.id}")

          channelMap(rdd.id) =
            new mutable.HashMap[Int, Receiver[Any]]()

          // Create a channel for each output partition
          val channels = Channel.createChannels[T](rdd.part.numPartitions).asScala

          // Create a sender for each input partition
          val senders = mutable.ArrayBuffer[Sender[Any]]()
          for (i <- 0 until split.inputPartitions.length) {
            senders += new Sender(channels.toArray).asInstanceOf[Sender[Any]]
          }

          // Create sender per input partitions for each output partition
          for (i <- 0 until rdd.part.numPartitions) {
            // println(s"rdd: ${rdd.id}, output partition: $i")

            // scalastyle:off println
            // println(s"rdd: ${rdd.id}, partition: $i, senders: ${senders.size}")

            channelMap(rdd.id).put(i, channels(i).createReceiver().asInstanceOf[Receiver[Any]])
          }

          // Launch one async task per *input* partition
          // scalastyle:off println
          // println(s"launch input tasks for rdd: ${rdd.id}, ")
          launchInputTasks(senders.toSeq, rdd, split, rdd.part, context)
          // println(s"launched input tasks for rdd: ${rdd.id}, ")
        } else {
          // scalastyle:off println
          // println(s"already has channel map for rdd: ${rdd.id}")
        }
      }

      context.addTaskCompletionListener((_) => {
        // scala:off println
        // println(s"task completed for rdd: ${rdd.id}, split: ${split.index}")
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
   *
   * @param rdd
   * @param part
   * @param context
   * @tparam T
   */
  def launchInputTasks[T](
      senders: Seq[Sender[Any]],
      rdd: LocalRepartitionRDD[T],
      split: LocalRepartitionPartition,
      part: Partitioner,
      context: TaskContext): Unit = {
    val tasks = new mutable.ArrayBuffer[CompletableFuture[Void]]()
    for (i <- 0 until split.inputPartitions.length) {
      // Launch the task
      val inputIterator = rdd.rdd.iterator(split.inputPartitions(i), context)

      // TODO: error handling
      tasks += senders(i).send(inputIterator, part).getFuture(i, senderThreadExecutor)
    }

    // All sender tasks are completed. Close the senders.
    val task = CompletableFuture.allOf(tasks.toArray: _*).whenComplete((_, _) => {
      // scalastyle:off println
      // println(s"all input tasks completed for rdd: ${rdd.id}")
    })

    sawnedTasks(rdd.id) = task
  }
}
