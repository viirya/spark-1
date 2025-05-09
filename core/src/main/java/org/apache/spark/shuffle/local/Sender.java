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
package org.apache.spark.shuffle.local;

import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.Iterator;

import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

public class Sender<T> {
    private final Channel<T>[] channels;
    private boolean closed = false;
    private final TaskContext taskContext;
    private final SparkEnv env;
    private final int rddId;
    private int senderId;
    private int senderQueueSize;

    private static AtomicInteger nextSenderId = new AtomicInteger(0);

    public Sender(int rddId, Channel<T>[] channels, int senderQueueSize, SparkEnv env, TaskContext taskContext) {
          this.rddId = rddId;
          this.channels  = channels;
          for (Channel<T> channel : channels) {
              channel.addSender();
          }
          this.taskContext = taskContext;
          this.env = env;
          this.senderId = nextSenderId.getAndIncrement();
          this.senderQueueSize = senderQueueSize;
    }

    public void close() {
        if (!closed) {
            for (Channel<T> channel : channels) {
                if (channel.reduceNumSenders() == 0) {
                    Waker receiverWaker;
                    try {
                        channel.lockChannel();

                        if (!channel.isClosed() && channel.isEmpty()) {
                            channel.getChannelGate().decrementEmptyChannelNumber();
                        }
                        receiverWaker = channel.getReceiverWaker();

                        // The channel cannot add a new receiver waker
                        channel.disableReceiverWaker();
                    } finally {
                        channel.unlockChannel();
                    }

                    if (receiverWaker != null) {
                        receiverWaker.wake();
                    }
                }
            }

            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public SenderFuture<T> send(byte[] task, Partition partition, Class<RDD<T>> clazz, Partitioner partitioner) {
        return new SenderFuture<>(senderId, rddId, task, partition, clazz,  this, channels, senderQueueSize, partitioner, env, taskContext);
    }
}
