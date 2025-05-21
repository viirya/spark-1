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
import java.util.Optional

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.{HashPartitioner, LocalSparkContext, Partitioner, SparkConf, SparkContext, SparkFunSuite, TaskContextImpl}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.rdd.{LocalRepartition, RDD}

class ReceiverSuite extends SparkFunSuite with LocalSparkContext with LocalRepartitionSuiteBase {
  test("receiver gets data from channel") {
    val channels = Channel.createChannels[Long](1, 10).asScala.toArray
    val receiver = channels(0).createReceiver(0, 1)

    var data: Optional[Long] = Optional.empty()
    // The receiver task will be blocked as no data in the channel
    val future = threadExecutor.submit(new Runnable {
      override def run(): Unit = {
        data = receiver.recv().get()
      }
    })

    // The receiver task is blocked by waiting for data
    assert(!future.isDone)
    channels(0).addData(0)
    channels(0).wakeReceivers()
    Eventually.eventually(Timeout(1.second)) {
      assert(future.isDone)
    }
    assert(data.get() == 0)
  }

  test("receiver receives data from sender through channel") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      val channels = Channel.createChannels[Long](1, 10).asScala.toArray

      val context = new TaskContextImpl(0, 0, 0, 0, 0, 1, null, null, null)
      val senderContext = LocalRepartition.createSenderTaskContext(context, 0, 1)
      val sender = new Sender(0, channels, 10, sc.env, senderContext).asInstanceOf[Sender[Any]]

      val rdd = sc.range(0, 10, 1, 1)
      val partitions = rdd.partitions
      val clazz = implicitly[ClassTag[RDD[Long]]].runtimeClass.asInstanceOf[Class[RDD[Any]]]
      val serializedRDD = getSerializedRDD(rdd)
      val partitioner = new HashPartitioner(1)

      val sendFuture = sender.send(serializedRDD, partitions(0), clazz, partitioner)
      val result = sendFuture.getFuture(threadExecutor).get()

      assert(result.isEmpty)
      assert(channels(0).getQueueSize == 10)

      // Don't allow the receiver to await
      channels(0).disableReceiverWaker()

      val receiver = channels(0).createReceiver(0, 10)
      val allData = new util.LinkedList[Long]()
      var data = receiver.recv().get()
      while (data.isPresent) {
        allData.add(data.get())
        data = receiver.recv().get()
      }
      assert(allData.asScala.toArray === Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    }
  }

  test("receiver receives data from senders through channel with multiple partitions") {
    val conf = new SparkConf()
    val sc = new SparkContext("local", "SenderSuite", conf)

    withSpark(sc) { sc =>
      // Create two channels
      val channels = Channel.createChannels[Long](2, 10).asScala.toArray

      // Create the receiver
      val receiver = channels(0).createReceiver(0, 10)

      var data: Optional[Long] = Optional.empty()
      // The receiver task will be blocked as no data in the channel
      val future = threadExecutor.submit(new Runnable {
        override def run(): Unit = {
          data = receiver.recv().get()
        }
      })

      // The receiver task is blocked by waiting for data
      assert(!future.isDone)

      // Create two senders
      val context = new TaskContextImpl(0, 0, 0, 0, 0, 2, null, null, null)
      val sender1Context = LocalRepartition.createSenderTaskContext(context, 0, 2)
      val sender2Context = LocalRepartition.createSenderTaskContext(context, 1, 2)
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

      val sendFuture1 = sender1.send(serializedRDD, partitions(0), clazz, partitioner)
      val result1 = sendFuture1.getFuture(threadExecutor).get()
      assert(result1.isEmpty)

      val sendFuture2 = sender2.send(serializedRDD, partitions(1), clazz, partitioner)
      val result2 = sendFuture2.getFuture(threadExecutor).get()
      assert(result2.isEmpty)

      // Once the senders begin sending data, the receiver will be unblocked
      Eventually.eventually(Timeout(5.seconds)) {
        assert(future.isDone)
      }
      assert(data.get() == 0)

      val allData = new util.LinkedList[Long]()
      var receivedData = receiver.recv().get()
      while (receivedData.isPresent) {
        allData.add(receivedData.get())
        receivedData = receiver.recv().get()
      }
      assert(allData.asScala.sorted.toArray === Array(2, 4, 6, 8))

      val channel2DataArray = channels(1).getAllData.asScala.toArray
      assert(channel2DataArray === Array(1, 3, 5, 7, 9))
    }
  }
}
