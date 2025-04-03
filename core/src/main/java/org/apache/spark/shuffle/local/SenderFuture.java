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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import scala.collection.Iterator;
import org.apache.spark.Partitioner;

public class SenderFuture<T> {
    private final Iterator<T> iterator;
    private final Sender<T> sender;
    private final Channel<T>[] channels;
    private final Partitioner partitioner;

    SenderFuture(Iterator<T> iterator, Sender<T> sender, Channel<T>[] channels, Partitioner partitioner) {
        this.iterator = iterator;
        this.sender = sender;
        this.channels = channels;
        this.partitioner = partitioner;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public CompletableFuture<Void> getFuture(int partId, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && iterator.hasNext() && !sender.isClosed()) {
                    // System.out.println("sender try to lock..." + ", channel id: " + channel.getId());
                    // System.out.println("sender: " + "channel id: " + channel.getId());
                    // channel.lock();
                    // System.out.println("sender got lock..." + ", channel id: " + channel.getId());

                    T data = iterator.next();
                    int key = partitioner.getPartition(data);
                    Channel<T> channel = channels[key];

                    // System.out.println("sender for data: " + data + " channel id: " + channel.getId());

                    // Check if empty channel number is 0, i.e., no receiver need data,
                    // then wait for the receiver to wake up the sender
                    if (channel.getChannelGate().getEmptyChannelNumber() == 0) {
                        Waker waker = getWaker();

                        if (channel.getChannelGate().addSenderWaker(waker, channel.getId())) {
                            int emptyChannelNumber = channel.getChannelGate().getEmptyChannelNumber();
                            // System.out.println("sender wait..." + " channel empty: " + channel.isEmpty() + " empty channel number: " + emptyChannelNumber + ", channel id: " + channel.getId());

                            // channel.unlock();
                            waker.await();
                            // System.out.println("sender woke up..." + ", channel id: " + channel.getId());
                        }
                    }


                    boolean readyToAdd = channel.readyToAdd();
                    // System.out.println("add data: " + data + ", waiting receivers: " + channel.getNumWakers() + " channel id: " + channel.getId());
                    channel.addData(data);

                    // channel.unlock();

                    // If data queue was empty before pushing new data, wake up the receivers
                    if (readyToAdd) {
                        channel.getChannelGate().decrementEmptyChannelNumber();
                        channel.wakeReceivers(false);
                    }
                }

                if (!iterator.hasNext()) {
                    // System.out.println("sender run out of data. " + ", partId id: " + partId);
                    sender.close();
                }

                return null;
            } catch (InterruptedException e) {
                return null;
            }
        }, executor);
    }
}