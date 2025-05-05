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

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.nio.ByteBuffer;

import scala.Option;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.serializer.SerializerInstance;

public class SenderFuture<T> {
    private final Sender<T> sender;
    private final Channel<T>[] channels;
    private final Partitioner partitioner;
    private final TaskContext taskContext;
    private final SparkEnv env;

    private final int senderId;
    private final int rddId;

    private int sentDataCount = 0;

    private Waker currentWaker;

    private byte[] task;
    private Partition partition;
    private Class<RDD<T>> clazz;

    SenderFuture(int senderId, int rddId, byte[] task, Partition partition, Class<RDD<T>> clazz, Sender<T> sender, Channel<T>[] channels, Partitioner partitioner, SparkEnv env, TaskContext taskContext) {
        this.senderId = senderId;
        this.rddId = rddId;
        this.sender = sender;
        this.channels = channels;
        this.partitioner = partitioner;
        this.taskContext = taskContext;
        this.env = env;
        this.task = task;
        this.partition = partition;
        this.clazz = clazz;
        this.currentWaker = getWaker();
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public Future<Optional<Throwable>> getFuture(ExecutorService executor) {

        return executor.submit(() -> {
            // Set the task context for the current thread
            TaskContext$.MODULE$.setTaskContext(taskContext);

            // Deserialize the task binary
            SerializerInstance ser = env.closureSerializer().newInstance();
            ClassTag<RDD<T>> tag = ClassTag$.MODULE$.apply(clazz);
            RDD<T> rdd = ser.deserialize(ByteBuffer.wrap(task), Thread.currentThread().getContextClassLoader(), tag);
            Iterator<T> iterator = rdd.iterator(partition, taskContext);

            Channel<T> channel = null;
            try {
                while (!Thread.currentThread().isInterrupted() && iterator.hasNext()) {
                    T data = iterator.next();
                    int key = partitioner.getPartition(data);
                    System.out.println("sender " + senderId + " rdd " + rddId + "sending data to channel " + key);
                    channel = channels[key];

                    boolean channelLocked = false;
                    try {
                        channel.lockChannel();
                        channelLocked = true;

                        // todo: better stop condition
                        if (channel.isClosed()) {
                            break;
                        }

                        if (channel.getChannelGate().getEmptyChannelNumber() == 0 && channel.isReachedMaxQueueSize()) {
                            boolean toWait;
                            try {
                                channel.getChannelGate().lockGate();
                                toWait = channel.getChannelGate().addSenderWaker(currentWaker, channel.getId());
                            } finally {
                                channel.getChannelGate().unlockGate();
                            }

                            if (toWait) {
                                channel.unlockChannel();
                                channelLocked = false;
                                System.out.println("sender " + senderId + " rdd " + rddId + " waiting on channel " + channel.getId());
                                currentWaker.await();
                                // Update the current waker after being woken up
                                currentWaker = getWaker();

                                channel.lockChannel();
                                channelLocked = true;
                            }
                        }

                        boolean wasEmpty = channel.isEmpty();
                        channel.addData(data);
                        sentDataCount += 1;

                        if (wasEmpty) {
                            channel.getChannelGate().decrementEmptyChannelNumber();
                            channel.wakeReceivers();
                        }
                    } finally {
                        if (channelLocked) {
                            channel.unlockChannel();
                        }
                    }
                }

                sender.close();

                return Optional.empty();
            } catch (Throwable e) {
                if (channel != null) {
                    channel.setError(e);
                }
                sender.close();
                return Optional.of(e);
            } finally {
                System.out.println("sender " + senderId + " finished rdd " + rddId + " sent data count: " + sentDataCount);
                taskContext.markTaskCompleted(Option.empty());

                // See `Task.scala` and `Executor.scala` for the details of the task lifecycle.
                try {
                    env.blockManager().memoryStore().releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, Long.MAX_VALUE);
                    env.blockManager().memoryStore().releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP, Long.MAX_VALUE);
                    MemoryManager memoryManager = env.blockManager().memoryManager();
                    synchronized (memoryManager) {
                        env.blockManager().memoryManager().notifyAll();
                    }
                } finally {
                    env.blockManager().releaseAllLocksForTask(taskContext.taskAttemptId());
                    taskContext.taskMemoryManager().cleanUpAllAllocatedMemory();
                    TaskContext$.MODULE$.unset();
                }
            }
        });
    }
}