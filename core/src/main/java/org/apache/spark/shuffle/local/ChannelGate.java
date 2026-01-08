
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

/**
 * During data exchange between senders, receivers through channels, this class manages the channel
 * states. It keeps track of the number of empty channels and manages the wakers for senders.
 * <p>
 * The class is thread-safe and uses locks to protect shared resources.
 */
public class ChannelGate {
  private AtomicInteger emptyChannelCounter;
  // Null after waking all senders, initiated when all channels become full
  // (i.e., we will begin to add sender wakers
  private LinkedList<Map.Entry<Waker, Integer>> senderWakers;
  private final ReentrantLock lock = new ReentrantLock();

  ChannelGate(int numChannels) {
    this.senderWakers = null;
    this.emptyChannelCounter = new AtomicInteger(numChannels);
  }


  /**
   * Wakes up all senders that are waiting for the specified channel.
   *
   * @param channelId the ID of the channel to wake up senders for.
   */
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

  /**
   * Adds all waiting sender wakers to the provided list. Cleans up stored wakers.
   *
   * @param wakers a list to store the wakers for the senders.
   */
  void getSenderWakers(LinkedList<Map.Entry<Waker, Integer>> wakers) {
    if (senderWakers != null) {
      wakers.addAll(senderWakers);
      // Clear the list before nulling to help GC release waker references sooner
      senderWakers.clear();
      senderWakers = null;
    }
  }

  /**
   * Increments the number of empty channels.
   *
   * @return the new number of empty channels.
   */
  int incrementEmptyChannelNumber() {
    return emptyChannelCounter.getAndAdd(1);
  }

  /**
   * Decrements the number of empty channels.
   * If the number of empty channels reaches zero and there are no sender wakers,
   * a new list for sender wakers is created. I.e., if all channels are full,
   * the senders will be possibly added to the waker list.
   */
  void decrementEmptyChannelNumber() {
    try {
      lock.lock();
      int newCount = emptyChannelCounter.decrementAndGet();
      // Initialize sender waker list when all channels become full
      if (newCount == 0 && senderWakers == null) {
        senderWakers = new LinkedList<>();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns the number of empty channels.
   *
   * @return the number of empty channels.
   */
  int getEmptyChannelNumber() {
    return emptyChannelCounter.get();
  }

  void lockGate() {
    lock.lock();
  }

  void unlockGate() {
    lock.unlock();
  }

  /**
   * Adds a sender waker to the list of sender wakers.
   *
   * @param waker     the waker to add.
   * @param channelId the ID of the channel associated with the waker.
   * @return true if the waker was added, false otherwise.
   */
  boolean addSenderWaker(Waker waker, int channelId) {
    if (senderWakers != null) {
      senderWakers.add(new AbstractMap.SimpleEntry<>(waker, channelId));
      return true;
    } else {
      return false;
    }
  }
}
