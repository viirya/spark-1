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
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Channel<T> {
    private final int id;
    private AtomicBoolean closed = new AtomicBoolean(false);;
    private final LinkedList<T> queue;
    private AtomicBoolean canAddReceiverWaker = new AtomicBoolean(true);
    private final ChannelGate channelGate ;
    private final AtomicInteger numSenders = new AtomicInteger(0);
    private final int queueSize;

    private final ReentrantLock lock = new ReentrantLock();

    private Waker currentWaker;

    private Optional<Throwable> error = Optional.empty();

    public static <T> List<Channel<T>> createChannels(int numChannels, int queueSize, int numSenders) {
        List<Channel<T>> channels = new LinkedList<>();
        ChannelGate channelGate = new ChannelGate(numChannels, numSenders);

        for (int i = 0; i < numChannels; i++) {
            channels.add(new Channel<>(i, channelGate, queueSize));
        }

        return channels;
    }

    Channel(int id, ChannelGate channelGate, int queueSize) {
        this.id = id;
        this.queue = new LinkedList<>();
        this.channelGate = channelGate;
        this.queueSize = queueSize;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void close() {
        try {
            lock.lock();

            closed.set(true);

            if (queue.size() < queueSize) {
                channelGate.decrementEmptyChannelNumber();
            }

            channelGate.wakeSenders();
            wakeReceivers(true);
        } finally {
            lock.unlock();
        }
    }

    int getId() {
        return id;
    }

    public boolean isError() {
        return error.isPresent();
    }

    public Optional<Throwable> getError() {
        return error;
    }

    void setError(Throwable error) {
        this.error = Optional.of(error);
        closed.set(true);
        channelGate.wakeSenders();
        wakeReceivers(true);
    }

    public void addSender() {
        numSenders.incrementAndGet();
    }

    public int reduceNumSenders() {
        return numSenders.decrementAndGet();
    }

    public int getNumSenders() {
        return numSenders.get();
    }

    public Receiver<T> createReceiver(int rddId) {
        return new Receiver<>(this, rddId);
    }

    public void wakeReceivers(boolean last) {
        if (last) {
            canAddReceiverWaker.set(false);
        }

        if (currentWaker == null) {
            return;
        }
        currentWaker.wake();
    }

    void addDataAndUpdateEmptyFlag(T data) {
        try {
            lock.lock();

            boolean status = queue.size() == queueSize - 1;
            queue.add(data);

            if (status) {
                // If data queue was filled after adding new data, decrease the empty channel number
                channelGate.decrementEmptyChannelNumber();
            }
        } finally {
            lock.unlock();
        }
    }

    T getDataAndUpdateEmptyFlag() {
        try {
            lock.lock();

            if (queue.size() == queueSize) {
                System.out.println("Gotta increase empty channel number, channel: " + id + ", queue size: " + getQueueSize() + ", senders: " + getNumSenders());
                if (channelGate.incrementEmptyChannelNumber()) {
                    System.out.println("Receiver wake up senders, channel: " + id + ", queue size: " + getQueueSize() + ", senders: " + getNumSenders());
                }
            }

            T data = queue.poll();

            return data;
        } finally {
            lock.unlock();
        }
    }


    int getQueueSize() {
        return queue.size();
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    public ChannelGate getChannelGate() {
        return channelGate;
    }

    void setCurrentWaker(Waker waker) {
        if (canAddReceiverWaker.get()) {
            currentWaker = waker;
        } else {
            currentWaker = null;
        }
    }

    boolean receiverWait() throws InterruptedException {
        if (currentWaker != null) {
            currentWaker.await();
            currentWaker = null;
            return true;
        } else {
            return false;
        }
    }
}

