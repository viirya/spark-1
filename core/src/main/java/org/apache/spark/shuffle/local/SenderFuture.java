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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import scala.collection.Iterator;

import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

public class SenderFuture<T> {
    private final Iterator<T> iterator;
    private final Sender<T> sender;
    private final Channel<T>[] channels;
    private final Partitioner partitioner;
    private final TaskContext taskContext;

    SenderFuture(Iterator<T> iterator, Sender<T> sender, Channel<T>[] channels, Partitioner partitioner, TaskContext taskContext) {
        this.iterator = iterator;
        this.sender = sender;
        this.channels = channels;
        this.partitioner = partitioner;
        this.taskContext = taskContext;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public CompletableFuture<Optional<Throwable>> getFuture(Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            // Set the task context for the current thread
            TaskContext$.MODULE$.setTaskContext(taskContext);

            Channel<T> channel = null;
            try {
                while (!Thread.currentThread().isInterrupted() && iterator.hasNext() && !sender.isClosed()) {
                    T data = iterator.next();
                    int key = partitioner.getPartition(data);
                    channel = channels[key];

                    if (channel.isClosed()) {
                        throw new IllegalStateException("Channel is closed");
                    }

                    // Check if empty channel number is 0, i.e., no receiver need data,
                    // then wait for the receiver to wake up the sender
                    if (channel.getChannelGate().getEmptyChannelNumber() == 0) {
                        Waker waker = getWaker();

                        if (channel.getChannelGate().addSenderWaker(waker, channel.getId())) {
                            waker.await();
                        }
                    }

                    boolean readyToAddBefore = channel.readyToAdd();
                    channel.addData(data);

                    // If data queue was filled after adding new data, decrease the empty channel number
                    if (readyToAddBefore != channel.readyToAdd()) {
                        channel.getChannelGate().decrementEmptyChannelNumber();
                    }

                    // If data queue was empty before pushing new data, wake up the receivers
                    if (!channel.isEmpty()) {
                        channel.wakeReceivers(false);
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
            }
        }, executor);
    }
}