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

class SenderFuture<T> {
    private final T data;
    private final Channel<T> channel;

    SenderFuture(T data, Channel<T> channel) {
        this.data = data;
        this.channel = channel;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    CompletableFuture<Boolean> getFuture() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (true) {
                    // Check if empty channel number is 0, i.e., no receiver need data,
                    // then wait for the receiver to wake up the sender
                    if (channel.getChannelGate().getEmptyChannelNumber() == 0) {
                        Waker waker = getWaker();
                        channel.getChannelGate().addSenderWaker(waker);
                        waker.await();
                    }

                    boolean isEmpty = channel.isEmpty();
                    channel.addData(data);

                    // If data queue was empty before pushing new data, wake up the receivers
                    if (isEmpty) {
                        channel.getChannelGate().decrementEmptyChannelNumber();
                        channel.wakeReceivers();
                        return true;
                    }
                }
            } catch (InterruptedException e) {
                return false;
            }
        });
    }
}