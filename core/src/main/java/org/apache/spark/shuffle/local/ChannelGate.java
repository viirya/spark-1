
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
    private AtomicInteger emptyChannelCounter = new AtomicInteger(0);
    private LinkedList<Map.Entry<Waker, Integer>> senderWakers;
    private final ReentrantLock lock = new ReentrantLock();

    ChannelGate(int emptyChannelNumber) {
        this.senderWakers = new LinkedList<>();
        this.emptyChannelCounter = new AtomicInteger(emptyChannelNumber);
    }

    boolean addSenderWaker(Waker waker, int channelId) {
        lock.lock();

        if (senderWakers != null) {
            senderWakers.add(new AbstractMap.SimpleEntry<>(waker, channelId));

            lock.unlock();
            return true;
        }

        lock.unlock();
        return false;
    }

    void wakeSenders() {
        lock.lock();

        if (emptyChannelCounter.get() > 0) {
            if (senderWakers != null) {
                for (Map.Entry<Waker, Integer> waker : senderWakers) {
                    waker.getKey().wake();
                }
            }
            senderWakers = null;
        }

        lock.unlock();
    }

    void wakeSenders(int channelId) {
        lock.lock();

        if (senderWakers == null) {
            lock.unlock();
            return;
        }

        for (Map.Entry<Waker, Integer> waker : senderWakers) {
            if (waker.getValue() == channelId) {
                waker.getKey().wake();

                senderWakers.remove(waker);
            }
        }
        lock.unlock();
    }

    int incrementEmptyChannelNumber() {
        return emptyChannelCounter.getAndAdd(1);
    }

    void decrementEmptyChannelNumber() {
        int oldCount = emptyChannelCounter.getAndAdd(-1);

        if (oldCount == 1) {
            lock.lock();

            if (emptyChannelCounter.get() == 0 && senderWakers != null) {
                senderWakers = new LinkedList<>();
            }

            lock.unlock();
        }
    }

    int getEmptyChannelNumber() {
        return emptyChannelCounter.get();
    }
}
