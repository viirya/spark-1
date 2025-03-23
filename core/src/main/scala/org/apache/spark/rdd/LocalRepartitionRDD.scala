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
    part: Partitioner)
  extends RDD[T](sc, Nil) {

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new LocalRepartitionDependency(rdd))
  }

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
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
  val channelMap = new mutable.HashMap[Int,
    mutable.HashMap[Int, (mutable.ArrayBuffer[Sender[Any]], Receiver[Any])]]()

  def initChannelMap[T](rdd: LocalRepartitionRDD[T], inputPartNums: Int): Unit = {
    channelMap.synchronized {
      if (!channelMap.contains(rdd.id)) {
        channelMap(rdd.id) =
          new mutable.HashMap[Int, (mutable.ArrayBuffer[Sender[Any]], Receiver[Any])]()

        val channels = Channel.createChannels[T](rdd.getNumPartitions).asScala

        for (i <- 0 until rdd.getNumPartitions) {
          val senders = mutable.ArrayBuffer[Sender[Any]]()
          for (_ <- 0 until inputPartNums) {
            senders += channels(i).createSender().asInstanceOf[Sender[Any]]
          }
          channelMap(rdd.id).put(i,
            (senders, channels(i).createReceiver().asInstanceOf[Receiver[Any]]))
        }
      }
    }
  }
}
