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

import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ReceiverFuture<T> {
    private final Channel<T> channel;
    private final int rddId;
    private final int receiverId;
    private final AtomicInteger received;

    ReceiverFuture(int receiverId, Channel<T> channel, int rddId, AtomicInteger received) {
        this.receiverId = receiverId;
        this.channel = channel;
        this.rddId = rddId;
        this.received = received;
    }

    Waker getWaker() {
        return new SimpleWaker();
    }

    public Optional<T> get() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (channel.isError()) {
                    throw new IllegalStateException("Error in channel", channel.getError().get());
                }

                boolean channelLocked = false;
                try {
                    channel.lockChannel();
                    channelLocked = true;
                    if (!channel.isEmpty()) {
                        T data = channel.getData();

                        if (channel.isEmpty() && channel.isReceiverWakerEnabled()) {
                            int oldCount = channel.getChannelGate().incrementEmptyChannelNumber();
                            if (oldCount == 0) {
                                LinkedList<Map.Entry<Waker, Integer>> wakers = new LinkedList<>();
                                try {
                                    channel.getChannelGate().lockGate();
                                    if (channel.getChannelGate().getEmptyChannelNumber() > 0) {
                                        channel.getChannelGate().getSenderWakers(wakers);
                                    }
                                } finally {
                                    channel.getChannelGate().unlockGate();
                                    channel.unlockChannel();
                                    channelLocked = false;
                                }

                                for (Map.Entry<Waker, Integer> waker : wakers) {
                                    waker.getKey().wake();
                                }
                            }
                        }

                        return Optional.of(data);
                    } else {
                        if (channel.isReceiverWakerEnabled()) {
                            Waker receiverWaker = getWaker();
                            channel.setCurrentWaker(receiverWaker);
                            channel.unlockChannel();
                            channelLocked = false;
                            receiverWaker.await();
                        } else {
                            return Optional.empty();
                        }
                    }
                } finally {
                    if (channelLocked) {
                        channel.unlockChannel();
                    }
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
