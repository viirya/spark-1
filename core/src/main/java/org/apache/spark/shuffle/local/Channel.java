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

    void lockChannel() {
        lock.lock();
    }

    void unlockChannel() {
        lock.unlock();
    }

    public boolean isClosed() {
        return closed.get();
    }

    void setClosed() {
        canAddReceiverWaker.set(false);
        this.closed.set(true);
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
        setClosed();

        this.error = Optional.of(error);
        channelGate.wakeSenders(id);
        wakeReceivers();
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

    public void wakeReceivers() {
        if (currentWaker == null) {
            return;
        }
        currentWaker.wake();
        currentWaker = null;
    }

    void addData(T data) {
        queue.add(data);
    }

    T getData() {
        return queue.poll();
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    public ChannelGate getChannelGate() {
        return channelGate;
    }

    void cleanUp() {
        queue.clear();
    }

    void disableReceiverWaker() {
        canAddReceiverWaker.set(false);
        currentWaker = null;
    }

    boolean isReceiverWakerEnabled() {
        return canAddReceiverWaker.get();
    }

    Waker getReceiverWaker() {
        return currentWaker;
    }

    void setCurrentWaker(Waker currentWaker) {
        this.currentWaker = currentWaker;
    }
}

