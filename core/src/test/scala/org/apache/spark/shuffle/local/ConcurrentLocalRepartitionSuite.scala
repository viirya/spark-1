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

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.{HashPartitioner, LocalSparkContext, Partition, Partitioner, SparkConf, SparkContext, SparkException, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.rdd.{LocalRepartition, RDD}

/**
 * Test suite for concurrent scenarios in local repartition including:
 * - Multiple senders/receivers operating simultaneously
 * - Channel contention and backpressure
 * - Error propagation in concurrent context
 */
class ConcurrentLocalRepartitionSuite extends SparkFunSuite
  with LocalSparkContext
  with LocalRepartitionSuiteBase {

  import LocalSparkContext._

  test("multiple senders concurrently send to same channel") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val numSenders = 5
      val channels = Channel.createChannels[Long](1, 100).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, numSenders, null, null, null)
      val senders = (0 until numSenders).map { i =>
        val senderContext = LocalRepartition
          .createSenderTaskContext(context, i, numSenders, 1000L + i)
        new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]
      }

      val rdd = sc.range(0, 100, 1, numSenders)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val futures = (0 until numSenders).map { i =>
        senders(i).send(serializedRDD, partitions(i), clazz, partitioner, null)
          .getFuture(threadExecutor)
      }

      // Wait for all senders to complete
      futures.foreach(_.get())

      // Verify all data arrived
      assert(channels(0).getQueueSize == 100)
    }
  }

  test("senders handle channel closure gracefully") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 100, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val future = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
        .getFuture(threadExecutor)

      // Close channel after sender starts
      Eventually.eventually(Timeout(2.seconds)) {
        assert(channels(0).getQueueSize > 0)
        channels(0).setClosed()
      }

      // Sender should complete without error even though channel was closed
      val result = future.get()
      assert(result.isEmpty, "Sender should complete successfully even with closed channel")
    }
  }

  test("receiver handles sender errors") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 10).asScala.toArray
      val receiver = channels(0).createReceiver(0, 10)

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = new ErrorRDD(sc, "Test error")
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
        .getFuture(threadExecutor)

      // Receiver should get error notification
      Eventually.eventually(Timeout(5.seconds)) {
        assert(channels(0).isError)
        assert(channels(0).getError.isPresent)
      }

      // Attempting to receive should throw
      val exception = intercept[IllegalStateException] {
        receiver.recv().get()
      }
      assert(exception.getMessage.contains("Error in channel"))
    }
  }

  test("multiple receivers can close independently") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val numChannels = 3
      val channels = Channel.createChannels[Long](numChannels, 10).asScala.toArray

      val receivers = channels.map(_.createReceiver(0, 10))

      // Close receivers in random order
      receivers(1).close()
      assert(receivers(1).isClosed)
      assert(!receivers(0).isClosed)
      assert(!receivers(2).isClosed)

      receivers(0).close()
      assert(receivers(0).isClosed)
      assert(!receivers(2).isClosed)

      receivers(2).close()
      assert(receivers(2).isClosed)
    }
  }

  test("sender with very large buffer size") {
    val conf = new SparkConf()
      .set("spark.localRepartition.sender.buffer.size", "10000")
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 100).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10000, sc.env, senderContext)
        .asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 1000, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      val result = sendFuture.getFuture(threadExecutor).get()

      assert(result.isEmpty)
      assert(channels(0).getQueueSize == 1000)
    }
  }

  test("receiver with very small buffer size") {
    val conf = new SparkConf()
      .set("spark.localRepartition.receiver.buffer.size", "1")
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 100).asScala.toArray
      val receiver = channels(0).createReceiver(0, 1) // Very small buffer

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
        .getFuture(threadExecutor)

      // Receiver should be able to receive all data despite small buffer
      val received = scala.collection.mutable.ArrayBuffer[Long]()
      Eventually.eventually(Timeout(5.seconds)) {
        var continue = true
        while (continue) {
          val data = receiver.recv().get()
          if (data.isPresent) {
            received += data.get().asInstanceOf[Long]
          } else {
            continue = false
          }
        }
        assert(received.length == 10)
      }
    }
  }

  test("channel gate handles rapid state changes") {
    val gate = new ChannelGate(5)

    // Rapidly change state
    for (_ <- 0 until 1000) {
      gate.decrementEmptyChannelNumber()
      if (gate.getEmptyChannelNumber <= 0) {
        gate.incrementEmptyChannelNumber()
      }
    }

    // Should still be in valid state
    assert(gate.getEmptyChannelNumber >= 0)
  }

  test("sender handles custom partitioner") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "ConcurrentSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](3, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1, 1000L)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 30, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)

      // Custom partitioner: 0-9 -> partition 0, 10-19 -> partition 1, 20-29 -> partition 2
      val partitioner = new Partitioner {
        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
          key match {
            case i: Long => (i / 10).toInt
            case _ => 0
          }
        }
      }

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner, null)
      sendFuture.getFuture(threadExecutor).get()

      // Verify partitioning
      assert(channels(0).getQueueSize == 10)
      assert(channels(1).getQueueSize == 10)
      assert(channels(2).getQueueSize == 10)

      // Verify data in each partition
      channels(0).getAllData.asScala.foreach { data =>
        val value = data.asInstanceOf[Long]
        assert(value >= 0 && value < 10)
      }
      channels(1).getAllData.asScala.foreach { data =>
        val value = data.asInstanceOf[Long]
        assert(value >= 10 && value < 20)
      }
      channels(2).getAllData.asScala.foreach { data =>
        val value = data.asInstanceOf[Long]
        assert(value >= 20 && value < 30)
      }
    }
  }
}

/**
 * Test RDD that throws an error when computed.
 */
class ErrorRDD(sc: SparkContext, errorMsg: String) extends RDD[Long](sc, Seq.empty) {
  override def getPartitions: Array[Partition] = Array(new Partition {
    override def index: Int = 0
  })

  override def compute(split: Partition, context: TaskContext): Iterator[Long] = {
    throw new SparkException(errorMsg)
  }
}
