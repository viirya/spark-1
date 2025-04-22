
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

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelGate {
    private AtomicInteger emptyChannelCounter;
    private ConcurrentLinkedQueue<Map.Entry<Waker, Integer>> senderWakers;
    private final ReentrantLock lock = new ReentrantLock();

    ChannelGate(int numChannel, int numSenders) {
        this.senderWakers = new ConcurrentLinkedQueue<>();
        this.emptyChannelCounter = new AtomicInteger(numChannel);
    }

    void wakeSenders() {
        try {
            lock.lock();

            if (senderWakers != null) {
                for (Map.Entry<Waker, Integer> waker : senderWakers) {
                    waker.getKey().wake();
                    senderWakers.remove(waker);
                }
                senderWakers = null;
            }
        } finally {
            lock.unlock();
        }
    }

    boolean incrementEmptyChannelNumber() {
        try {
            boolean woke = false;

            lock.lock();

            int oldCount = emptyChannelCounter.getAndAdd(1);
            if (oldCount == 0) {
                wakeSenders();
                woke = true;
            }

            return woke;
        } finally {
            lock.unlock();
        }
    }

    void decrementEmptyChannelNumber() {
        try {
            lock.lock();

            int oldCount = emptyChannelCounter.getAndAdd(-1);
            if (oldCount == 1) {
                senderWakers = new ConcurrentLinkedQueue<>();
            }
        } finally {
            lock.unlock();
        }
    }

    boolean checkAndAddSenderWaker(int senderId, Waker waker, int channelId) {
        try {
            lock.lock();

            if (emptyChannelCounter.get() == 0) {
                senderWakers.add(new AbstractMap.SimpleEntry<>(waker, channelId));
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }
}
