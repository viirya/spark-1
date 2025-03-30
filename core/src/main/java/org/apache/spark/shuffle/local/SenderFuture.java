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

public class SenderFuture<T> {
    private final T data;
    private final Sender<T> sender;
    private final Channel<T> channel;

    SenderFuture(T data, Sender<T> sender, Channel<T> channel) {
        this.data = data;
        this.sender = sender;
        this.channel = channel;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public CompletableFuture<Boolean> getFuture(Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && !channel.isClosed() && !sender.isClosed()) {
                    // System.out.println("sender try to lock..." + ", channel id: " + channel.getId());
                    // channel.lock();
                    // System.out.println("sender got lock..." + ", channel id: " + channel.getId());

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
                            continue;
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
                    return true;
                }
                return false;
            } catch (InterruptedException e) {
                return false;
            }
        }, executor);
    }
}