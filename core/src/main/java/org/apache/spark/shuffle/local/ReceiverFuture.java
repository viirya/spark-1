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
import java.util.Optional;

public class ReceiverFuture<T> {
    private final Channel<T> channel;

    ReceiverFuture(Channel<T> channel) {
        this.channel = channel;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public CompletableFuture<Optional<T>> getFuture(Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && !channel.isClosed()) {
                    // System.out.println("receiver try to lock..." + ", channel id: " + channel.getId());
                    // System.out.println("receiver: " + "channel id: " + channel.getId());
                    // channel.lock();
                    // System.out.println("receiver got lock..." + ", channel id: " + channel.getId());

                    if (!channel.isEmpty()) {
                        T data = channel.getData();

                        // System.out.println("get data: " + data  + ", channel id: " + channel.getId());

                        boolean readyToAdd = channel.readyToAdd();
                        if (readyToAdd) {
                            // Check if all channels are filled with data before pulling data,
                            // if so, wake up the waiting senders.
                            int oldCount = channel.getChannelGate().incrementEmptyChannelNumber();
                            if (oldCount == 0) {
                                channel.getChannelGate().wakeSenders();
                            }
                        }

                        // channel.unlock();
                        return Optional.of(data);
                    } else if (channel.getNumSenders() > 0) {
                        if (channel.getNumSenders() > 0) {
                            // Hold this receiver to wait for the sender to wake up the receiver
                            Waker waker = getWaker();
                            if (channel.addReceiverWaker(waker)) {
                                // System.out.println("receiver wait..." + " channel senders: " + channel.getNumSenders() + " channel empty: " + channel.isEmpty() + ", channel id: " + channel.getId());
                                waker.await();
                            }
                        }

                        // System.out.println("receiver woke!" + ", channel id: " + channel.getId());
                    } else {
                        // System.out.println("Receiver exit. No senders. " + " channel empty: " + channel.isEmpty() + ", channel id: " + channel.getId());
                        // channel.unlock();
                        return Optional.empty();
                    }
                }
                return Optional.empty();
            } catch (Exception e) {
                // return Optional.empty();
                throw new RuntimeException(e);
            }
        }, executor);
    }

}
