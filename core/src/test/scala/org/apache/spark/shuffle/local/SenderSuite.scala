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

import java.util

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.{HashPartitioner, LocalSparkContext, Partition, Partitioner, SparkConf, SparkContext, SparkException, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.rdd.{LocalRepartition, RDD, SenderCallBack}

class SenderSuite extends SparkFunSuite with LocalSparkContext with LocalRepartitionSuiteBase {
  import LocalSparkContext._

  test("sender puts data into channel") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      val result = sendFuture.getFuture(threadExecutor).get()

      assert(result.isEmpty)
      assert(channels(0).getQueueSize == 10)
    }
  }

  test("sender captures error during computing rdd") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = new TestErrorRDD(sc)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      // SenderFuture returns Optional<Throwable>, where empty = success, non-empty = error
      val result = sendFuture.getFuture(threadExecutor).get()
      assert(result.isPresent, "Expected error to be captured in Optional")
      assert(result.get().isInstanceOf[SparkException],
        s"Expected SparkException but got ${result.get().getClass.getName}")
    }
  }

  test("sender put data into multiple channels based on partitioning") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      // Create two channels
      val channels = Channel.createChannels[Long](2, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 2, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new Partitioner {
        override def numPartitions: Int = 2

        override def getPartition(key: Any): Int = {
          key match {
            case i: Long => (i % 2).toInt
            case _ => throw new IllegalArgumentException("Key is not a Long")
          }
        }
      }

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      val result = sendFuture.getFuture(threadExecutor).get()
      assert(result.isEmpty)

      // Check that the data is distributed across the channels based on the partitioning
      val channel1 = channels(0)
      val channel2 = channels(1)
      val channel1Data = channel1.getQueueSize
      val channel2Data = channel2.getQueueSize
      assert(channel1Data == 5)
      assert(channel2Data == 5)

      // Check that the data in each channel is correct
      val channel1DataArray = channel1.getAllData.asScala.toArray
      val channel2DataArray = channel2.getAllData.asScala.toArray
      for (i <- 0 until channel1DataArray.length) {
        assert(channel1DataArray(i) % 2 == 0)
      }
      for (i <- 0 until channel2DataArray.length) {
        assert(channel2DataArray(i) % 2 == 1)
      }
    }
  }

  test("sender awaits when channel is full") {
    val conf = new SparkConf()
      .set("spark.localRepartition.buffer.size", "1")
      .set("spark.localRepartition.sender.buffer.size", "1")
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      // Create a channel with a max queue size of 1
      val channels = Channel.createChannels[java.lang.Long](1, 1).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 1, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[java.lang.Long]]]
        .runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      val future = sendFuture.getFuture(threadExecutor)
      assert(!future.isDone)

      Eventually.eventually(Timeout(1.second)) {
        assert(channels(0).getAllData.asScala.toArray === Array(0))
        assert(channels(0).isReachedMaxQueueSize)
      }

      // The sender is waiting
      assert(!future.isDone)

      val data = new util.LinkedList[java.lang.Long]
      Eventually.eventually(Timeout(10.seconds)) {
        val wakers: util.LinkedList[util.Map.Entry[Waker, Integer]] =
          new util.LinkedList[util.Map.Entry[Waker, Integer]]
        channels(0).getChannelGate.getSenderWakers(wakers)

        // Wake up the waiting sender
        for (waker <- wakers.asScala) {
          waker.getKey.wake()
        }

        val d: java.lang.Long = channels(0).getData
        if (d != null) {
          data.add(d)
        }

        assert(future.isDone)
        assert(data.size() == 10)
      }
    }
  }

  test("multiple senders put data into multiple channels") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      // Create two channels
      val channels = Channel.createChannels[Long](2, 10).asScala.toArray

      // Create two senders
      val context = new TaskContextImpl(0, 0, 0, 0, 0, 2, null, null, null)
      val sender1Context = LocalRepartition.createSenderTaskContext(context, 0, 2, 1000L)
      val sender2Context = LocalRepartition.createSenderTaskContext(context, 1, 2, 1001L)
      val sender1 = new Sender(0, channels, 10, sc.env, sender1Context).asInstanceOf[Sender[Any]]
      val sender2 = new Sender(0, channels, 10, sc.env, sender2Context).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 2)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new Partitioner {
        override def numPartitions: Int = 2

        override def getPartition(key: Any): Int = {
          key match {
            case i: Long => (i % 2).toInt
            case _ => throw new IllegalArgumentException("Key is not a Long")
          }
        }
      }

      val sendFuture1 = sender1.send(serializedRDD, partitions(0), clazz, partitioner, null)
      val result1 = sendFuture1.getFuture(threadExecutor).get()
      assert(result1.isEmpty)

      val sendFuture2 = sender2.send(serializedRDD, partitions(1), clazz, partitioner, null)
      val result2 = sendFuture2.getFuture(threadExecutor).get()
      assert(result2.isEmpty)

      // Check that the data is distributed across the channels based on the partitioning
      val channel1 = channels(0)
      val channel2 = channels(1)
      val channel1Data = channel1.getQueueSize
      val channel2Data = channel2.getQueueSize
      assert(channel1Data == 5)
      assert(channel2Data == 5)

      // Check that the data in each channel is correct
      val channel1DataArray = channel1.getAllData.asScala.toArray
      val channel2DataArray = channel2.getAllData.asScala.toArray
      for (i <- 0 until channel1DataArray.length) {
        assert(channel1DataArray(i) % 2 == 0)
      }
      for (i <- 0 until channel2DataArray.length) {
        assert(channel2DataArray(i) % 2 == 1)
      }
    }
  }

  test("sender can trigger other sender in callback") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      // Create two channels
      val channels = Channel.createChannels[Long](2, 10).asScala.toArray

      // Create two senders
      val context = new TaskContextImpl(0, 0, 0, 0, 0, 2, null, null, null)
      val sender1Context = LocalRepartition.createSenderTaskContext(context, 0, 2, 1000L)
      val sender2Context = LocalRepartition.createSenderTaskContext(context, 1, 2, 1001L)
      val sender1 = new Sender(0, channels, 10, sc.env, sender1Context).asInstanceOf[Sender[Any]]
      val sender2 = new Sender(0, channels, 10, sc.env, sender2Context).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 2)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new Partitioner {
        override def numPartitions: Int = 2

        override def getPartition(key: Any): Int = {
          key match {
            case i: Long => (i % 2).toInt
            case _ => throw new IllegalArgumentException("Key is not a Long")
          }
        }
      }

      val callBack = new SenderCallBack(Seq(sender1, sender2), 1,
        serializedRDD, partitions, clazz, partitioner, threadExecutor, null)

      // Trigger the first sender
      val sendFuture1 = sender1.send(serializedRDD, partitions(0), clazz, partitioner, callBack)
      val result1 = sendFuture1.getFuture(threadExecutor).get()
      assert(result1.isEmpty)

      // We don't manually trigger the second sender,
      // but it is triggered by the callback and completes
      val channel1 = channels(0)
      val channel2 = channels(1)
      Eventually.eventually(Timeout(10.seconds)) {
        val channel1Data = channel1.getQueueSize
        val channel2Data = channel2.getQueueSize
        assert(channel1Data == 5)
        assert(channel2Data == 5)
      }

      // Check that the data in each channel is correct
      val channel1DataArray = channel1.getAllData.asScala.toArray
      val channel2DataArray = channel2.getAllData.asScala.toArray
      for (i <- 0 until channel1DataArray.length) {
        assert(channel1DataArray(i) % 2 == 0)
      }
      for (i <- 0 until channel2DataArray.length) {
        assert(channel2DataArray(i) % 2 == 1)
      }
    }
  }
}

class TestErrorRDD(sc: SparkContext) extends RDD[Long](sc, Seq.empty) {
  override def getPartitions: Array[Partition] = Array(new Partition {
    override def index: Int = 0
  })

  override def compute(split: Partition, context: TaskContext): Iterator[Long] = {
    throw new SparkException("Test exception")
  }
}
