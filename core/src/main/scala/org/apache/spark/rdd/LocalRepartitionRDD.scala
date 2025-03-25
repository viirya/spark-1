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

import java.util.concurrent.CompletableFuture

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, LocalRepartitionDependency, Partition, Partitioner, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.local._

class LocalRepartitionPartition(rddId: Int, val index: Int) extends Partition {
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
    LocalRepartition.initiate(this, context)

    val tasks = LocalRepartition.sawnedTasks(id)
    val task = tasks(split.index)

    Iterator.empty
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
      result(i) = new LocalRepartitionPartition(id, i)
    }

    result
  }
}

object LocalRepartition {
  /**
   * A map to store the channels for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a map from output partition index to a pair of
   * (senders, receiver).
   */
  val channelMap = new mutable.HashMap[Int,
    mutable.HashMap[Int, (mutable.ArrayBuffer[Sender[Any]], Receiver[Any])]]()

  /**
   * A map to store the async tasks for each LocalRepartitionRDD.
   * The key is the RDD ID, and the value is a sequence of async tasks, one per input partition.
   */
  val sawnedTasks = mutable.HashMap[Int, Seq[CompletableFuture[Unit]]]()

  /**
   * Initialize the channel map for the given LocalRepartitionRDD.
   * This method is thread-safe.
   * @param rdd
   * @tparam T
   */
  def initiate[T](rdd: LocalRepartitionRDD[T], context: TaskContext): Unit = {
    channelMap.synchronized {
      if (!channelMap.contains(rdd.id)) {
        channelMap(rdd.id) =
          new mutable.HashMap[Int, (mutable.ArrayBuffer[Sender[Any]], Receiver[Any])]()

        // Create a channel for each output partition
        val channels = Channel.createChannels[T](rdd.getNumPartitions).asScala

        // Create sender per input partitions for each output partition
        for (i <- 0 until rdd.getNumPartitions) {
          val senders = mutable.ArrayBuffer[Sender[Any]]()
          for (_ <- 0 until rdd.rdd.getNumPartitions) {
            senders += channels(i).createSender().asInstanceOf[Sender[Any]]
          }
          channelMap(rdd.id).put(i,
            (senders, channels(i).createReceiver().asInstanceOf[Receiver[Any]]))
        }

        // Launch one async task per *input* partition
        launchInputTasks(rdd, rdd.part, context)
      }
    }
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
      rdd: LocalRepartitionRDD[T],
      part: Partitioner,
      context: TaskContext): Unit = {
    val tasks = new mutable.ArrayBuffer[CompletableFuture[Unit]]()
    for (i <- 0 until rdd.rdd.getNumPartitions) {
      val senders = mutable.HashMap[Int, Sender[Any]]()

      for (j <- 0 until rdd.getNumPartitions) {
        val sender = LocalRepartition.channelMap(rdd.id)(j)._1(i)
        senders(j) = sender
      }

      // Launch the task
      val inputIterator = rdd.iterator(rdd.rdd.partitions(i), context)
      tasks += createInputTask(inputIterator, part, senders.toMap)
    }

    // val allInputTasks = CompletableFuture.allOf(tasks.toArray: _*)

    sawnedTasks(rdd.id) = tasks.toSeq
  }

  def createInputTask[T](
      inputIterator: Iterator[T],
      part: Partitioner,
      outputChannels: Map[Int, Sender[Any]]): CompletableFuture[Unit] = {
    if (!inputIterator.hasNext) {
      CompletableFuture.completedFuture(null)
    }

    val item = inputIterator.next()
    val key = part.getPartition(item)
    val future = outputChannels(key).send(item).getFuture
    future.thenCompose(_ => createInputTask(inputIterator, part, outputChannels))
  }
}
