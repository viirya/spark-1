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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Channel<T> {
    private final int id;
    private boolean closed = false;
    private final ConcurrentLinkedQueue<T> queue;
    private AtomicBoolean canAddReceiverWaker = new AtomicBoolean(true);
    private ConcurrentLinkedQueue<Waker> receiverWakers;
    private final ChannelGate channelGate ;
    private final AtomicInteger numSenders = new AtomicInteger(0);
    private final int queueSize;

    public static <T> List<Channel<T>> createChannels(int numChannels, int queueSize) {
        List<Channel<T>> channels = new LinkedList<>();
        ChannelGate channelGate = new ChannelGate(numChannels);

        for (int i = 0; i < numChannels; i++) {
            channels.add(new Channel<>(i, channelGate, queueSize));
        }

        return channels;
    }

    Channel(int id, ChannelGate channelGate, int queueSize) {
        this.id = id;
        this.queue = new ConcurrentLinkedQueue<>();
        this.receiverWakers = new ConcurrentLinkedQueue<>();
        this.channelGate = channelGate;
        this.queueSize = queueSize;
    }

    public synchronized boolean isClosed() {
        return closed;
    }

    public synchronized void close() {
        closed = true;

        channelGate.wakeSenders();
        wakeReceivers(true);
    }

    int getId() {
        return id;
    }

    public void addSender() {
        numSenders.incrementAndGet();
    }

    public int reduceNumSenders() {
        return numSenders.decrementAndGet();
    }

    public Receiver<T> createReceiver() {
        return new Receiver<>(this);
    }

    boolean addReceiverWaker(Waker waker) {
        synchronized(canAddReceiverWaker) {
            if (canAddReceiverWaker.get()) {
                receiverWakers.add(waker);
                return true;
            } else {
                return false;
            }
        }
    }

    public void wakeReceivers(boolean last) {
        synchronized(canAddReceiverWaker) {
            for (Waker waker : receiverWakers) {
                waker.wake();
                receiverWakers.remove(waker);
            }
            if (last) {
                canAddReceiverWaker.set(false);
            }
        }
    }

    void addData(T data) {
        queue.add(data);
    }

    T getData() {
        return queue.poll();
    }

    boolean readyToAdd() {
        return queue.size() < queueSize;
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    public ChannelGate getChannelGate() {
        return channelGate;
    }
}

