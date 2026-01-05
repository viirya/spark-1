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

/**
 * A channel is a FIFO queue that is used by senders and receivers to exchange data asynchronously.
 * It allows multiple senders to send data to a single receiver, and multiple receivers to receive
 * data from a single sender. Senders can distribute data across multiple channels based on some
 * distribution logic, such as hash partitioning, range partitioning, or custom partitioning logic.
 * <p>
 * Only one sender can send data to a channel at a time, but multiple senders can send data to
 * different channels concurrently. Currently, a channel only supports one receiver. A channel keeps
 * track the current waker of the receiver, which is used to wake up the receiver when data is
 * available. When the receiver tries to receive data, it will block until data is available. Before
 * the receiver blocks, it will set the current waker to itself. When data is available, the sender
 * who added the data will wake up the receiver by calling the waker's wake method.
 * <p>
 * On the sender side, generally if the channel is full, the sender will block until the receiver
 * consumes some data and wakes up the sender. The fullness of the channel is determined by the
 * configuration of the maximum queue size.
 *
 * @param <T> The type of data that this channel will hold.
 */
public class Channel<T> {
  private final int id;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final LinkedList<T> queue;
  // A flag to indicate whether a receiver waker can be added.
  // Once a channel has no more senders, the last sender will turn this flag to false.
  private final AtomicBoolean canAddReceiverWaker = new AtomicBoolean(true);
  private final ChannelGate channelGate;
  private final AtomicInteger numSenders = new AtomicInteger(0);
  private final int queueSize;

  // A lock to protect the channel from concurrent access.
  private final ReentrantLock lock = new ReentrantLock();

  // A waker to wake up the receiver when data is available.
  private Waker currentWaker;

  private volatile Optional<Throwable> error = Optional.empty();

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

  boolean tryLockChannel() {
    return lock.tryLock();
  }

  public boolean isClosed() {
    return closed.get();
  }

  void setClosed() {
    canAddReceiverWaker.set(false);
    this.closed.set(true);
    // Wake up any senders that might be waiting
    channelGate.wakeSenders(id);
    wakeReceivers();
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
    // Set error first before closing/waking to ensure threads see error state
    this.error = Optional.of(error);

    // setClosed() will wake senders and receivers, ensuring they see the error
    setClosed();
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

  public Receiver<T> createReceiver(int rddId, int queueSize) {
    return new Receiver<>(this, rddId, queueSize);
  }

  /**
   * Returns the current receiver waker and reset it to null.
   * This is used to wake up the receiver when data is available.
   *
   * @return The current waker.
   */
  Waker getCurrentWaker() {
    Waker waker = currentWaker;
    currentWaker = null;
    return waker;
  }

  /**
   * Wake up the receiver if it is waiting for data.
   * This is used to notify the receiver that data is available.
   */
  void wakeReceivers() {
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

  // Only for testing
  public LinkedList<T> getAllData() {
    return queue;
  }

  boolean isEmpty() {
    return queue.isEmpty();
  }

  public int getQueueSize() {
    return queue.size();
  }

  boolean isReachedMaxQueueSize() {
    return queue.size() >= queueSize;
  }

  public ChannelGate getChannelGate() {
    return channelGate;
  }

  void cleanUp() {
    queue.clear();
  }

  /**
   * Disable the receiver waker. This is used to prevent the receiver from being into
   * waiting status when there are no more senders.
   */
  void disableReceiverWaker() {
    canAddReceiverWaker.set(false);
    currentWaker = null;
  }

  /**
   * Returns if it is possible to add a receiver waker into the channel.
   */
  boolean isReceiverWakerEnabled() {
    return canAddReceiverWaker.get();
  }

  /**
   * Sets the current waker to the given waker.
   *
   * @param currentWaker The waker to set as the current waker.
   */
  void setCurrentWaker(Waker currentWaker) {
    this.currentWaker = currentWaker;
  }
}

