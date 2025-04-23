
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
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ChannelGate {
    private AtomicInteger emptyChannelCounter;
    // Null after waking all senders, initiated when all channels become full
    // (i.e., we will begin to add sender wakers
    private LinkedList<Map.Entry<Waker, Integer>> senderWakers;
    private final ReentrantLock lock = new ReentrantLock();

    ChannelGate(int numChannel, int numSenders) {
        this.senderWakers = null;
        this.emptyChannelCounter = new AtomicInteger(numChannel);
    }

    void wakeSenders(int channelId) {
        LinkedList<Map.Entry<Waker, Integer>> wakers = new LinkedList<>();
        try {
            lock.lock();
            if (senderWakers != null) {
                for (Map.Entry<Waker, Integer> waker : senderWakers) {
                    if (waker.getValue() != channelId) {
                        continue;
                    }
                    wakers.add(waker);
                }
            }
        } finally {
            lock.unlock();
        }

        for (Map.Entry<Waker, Integer> waker : wakers) {
            waker.getKey().wake();
        }
    }

    void getSenderWakers(LinkedList<Map.Entry<Waker, Integer>> wakers) {
        if (senderWakers != null) {
            wakers.addAll(senderWakers);
            senderWakers = null;
        }
    }

    int getNumSenderWakers() {
        if (senderWakers != null) {
            return senderWakers.size();
        } else {
            return 0;
        }
    }

    int incrementEmptyChannelNumber() {
        return emptyChannelCounter.getAndAdd(1);
    }

    void decrementEmptyChannelNumber() {
        int oldCount = emptyChannelCounter.getAndAdd(-1);
        if (oldCount == 1) {
            try {
                lock.lock();
                if (emptyChannelCounter.get() == 0 && senderWakers == null) {
                    senderWakers = new LinkedList<>();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    int getEmptyChannelNumber() {
        return emptyChannelCounter.get();
    }

    void lockGate() {
        lock.lock();
    }

    void unlockGate() {
        lock.unlock();
    }

    boolean addSenderWaker(Waker waker, int channelId) {
        if (senderWakers != null) {
            senderWakers.add(new AbstractMap.SimpleEntry<>(waker, channelId));
            return true;
        } else {
            return false;
        }
    }
}
