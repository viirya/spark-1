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

public class Channel<T> {
    private final int id;
    private boolean closed = false;
    private int numSenders = 0;
    private final ConcurrentLinkedQueue<T> queue;
    private final ConcurrentLinkedQueue<Waker> receiverWakers;
    private final ChannelGate channelGate ;

    public static <T> List<Channel<T>> createChannels(int numChannels) {
        List<Channel<T>> channels = new LinkedList<>();
        ChannelGate channelGate = new ChannelGate();

        for (int i = 0; i < numChannels; i++) {
            channels.add(new Channel<>(i, channelGate));
        }

        return channels;
    }

    Channel(int id, ChannelGate channelGate) {
        this.id = id;
        this.queue = new ConcurrentLinkedQueue<>();
        this.receiverWakers = new ConcurrentLinkedQueue<>();
        this.channelGate = channelGate;
    }

    boolean isClosed() {
        return closed;
    }

    void close() {
        closed = true;
    }

    int getId() {
        return id;
    }

    synchronized int getNumSenders() {
        return numSenders;
    }

    synchronized Sender<T> createSender() {
        numSenders += 1;
        return new Sender<>(this);
    }

    synchronized Receiver<T> createReceiver() {
        return new Receiver<>(this);
    }

    void addReceiverWaker(Waker waker) {
        receiverWakers.add(waker);
    }

    void wakeReceivers() {
        for (Waker waker : receiverWakers) {
            waker.wake();
        }
        receiverWakers.clear();
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
}

