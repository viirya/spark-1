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
            channel.setCurrentWaker(getWaker());

            while (!Thread.currentThread().isInterrupted() && (!channel.isClosed() || !channel.isEmpty())) {
                if (channel.isError()) {
                    throw new IllegalStateException("Error in channel", channel.getError().get());
                }

                if (!channel.isEmpty()) {
                    System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") gotta get data from channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get());

                    T data = channel.getDataAndUpdateEmptyFlag();
                    if (data == null) {
                        System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") get null data from channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get());
                        return Optional.empty();
                    } else {
                        System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") get non-null data from channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get());
                    }
                    received.incrementAndGet();

                    return Optional.of(data);
                } else {
                    // Hold this receiver to wait for the sender to wake up the receiver
                    System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") trying to waiting for sender to wake up, channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get());

                    if (channel.receiverWait()) {
                        System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") is woken up, channel: " + channel.getId() + ", queue size: " + channel.getQueueSize());
                    } else {
                        // System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") decided not to waiting for sender to wake up, channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get());

                        if (false && channel.isEmpty()) {
                            System.out.println("Receiver: " + receiverId + " (rdd: " + rddId + ") cannot waiting for sender to wake up, channel: " + channel.getId() + ", queue size: " + channel.getQueueSize() + ", senders: " + channel.getNumSenders() + ", received: " + received.get() + ", no data");
                            return Optional.empty();
                        }
                    }
                }
            }
            return Optional.empty();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
