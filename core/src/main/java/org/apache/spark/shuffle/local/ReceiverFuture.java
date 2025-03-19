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

class ReceiverFuture<T> {
    private final Channel<T> channel;

    ReceiverFuture(Channel<T> channel) {
        this.channel = channel;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    CompletableFuture<T> getFuture() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (true) {
                    if (!channel.isEmpty()) {
                        T data = channel.getData();

                        boolean isEmpty = channel.isEmpty();
                        if (isEmpty) {
                            // Check if all channels are filled with data before pulling data,
                            // if so, wake up the waiting senders.
                            int oldCount = channel.getChannelGate().incrementEmptyChannelNumber();
                            if (oldCount == 0) {
                                channel.getChannelGate().wakeSenders();
                            }
                        }

                        return data;
                    } else {
                        // Hold this receiver to wait for the sender to wake up the receiver
                        Waker waker = getWaker();
                        channel.addReceiverWaker(waker);
                        waker.await();
                    }
                }
            } catch (InterruptedException e) {
                return null;
            }
        });
    }

}
