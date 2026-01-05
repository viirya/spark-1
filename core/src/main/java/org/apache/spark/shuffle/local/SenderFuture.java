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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.serializer.SerializerInstance;

/**
 * A class that represents a future for a sender operation. It handles the sending of data to
 * channels and manages the task context and memory management.
 * <p>
 * The future after initialization pulls data by executing specified partition of the given RDD and
 * sends it to the specified channels after partitioning it.
 * <p>
 * In order to improve throughput, the sender uses a queue for each channel internally. The queue
 * size is specified by the `senderQueueSize` parameter. The sender will not send data to the channel
 * if the queue size is less than `senderQueueSize`. This allows the sender to batch data before
 * sending it to the channel, which can improve performance by reducing the number of calls to the
 * channel.
 * <p>
 * If the queue is full, the sender will wait for the channel to become available before sending
 * data. It uses a waker to wake up the sender when the channel is available again.
 * <p>
 * The future completes once all data are pulled from the RDD partition or channels are closed by
 * finished receivers.
 *
 * @param <T> The type of data being sent.
 */
public class SenderFuture<T> {
  private static final Logger logger = LoggerFactory.getLogger(SenderFuture.class);

  // Threshold multiplier for aggressive queue flushing
  // When queue size is between senderQueueSize and senderQueueSize * this multiplier,
  // we use tryLock instead of blocking lock to avoid contention
  private static final double QUEUE_FLUSH_THRESHOLD_MULTIPLIER = 1.5;

  private final Sender<T> sender;
  private final Channel<T>[] channels;
  private final Partitioner partitioner;
  private final TaskContext taskContext;
  private final SparkEnv env;

  private final int senderId;
  private final int rddId;

  private Waker currentWaker;

  private byte[] task;
  private Partition partition;
  private Class<RDD<T>> clazz;

  private int senderQueueSize;
  private LinkedList<T>[] queues;

  private Callable<Void> callback;

  private boolean channelLocked = false;

  SenderFuture(int senderId, int rddId, byte[] task, Partition partition, Class<RDD<T>> clazz,
               Sender<T> sender, Channel<T>[] channels, int senderQueueSize,
               Partitioner partitioner, SparkEnv env, TaskContext taskContext, Callable<Void> callback) {
    this.senderId = senderId;
    this.rddId = rddId;
    this.sender = sender;
    this.channels = channels;
    this.partitioner = partitioner;
    this.taskContext = taskContext;
    this.env = env;
    this.task = task;
    this.partition = partition;
    this.clazz = clazz;
    this.currentWaker = getWaker();

    this.senderQueueSize = senderQueueSize;

    // Initialize the queues for each channel
    this.queues = new LinkedList[channels.length];
    for (int i = 0; i < channels.length; i++) {
      this.queues[i] = new LinkedList<>();
    }

    this.callback = callback;
  }

  Waker getWaker() {
    return new SimpleWaker();
  }

  State nextQueue() {
    // Try to find a queue that has data and a channel that is not locked, in three attempts.
    boolean hasNonEmptyQueue = false;
    int queueId = -1;
    for (int i = 0; i < queues.length; i++) {
      LinkedList<T> queue = queues[i];
      Channel<T> channel = channels[i];
      if (!queue.isEmpty()) {
        hasNonEmptyQueue = true;
        queueId = i;
        if (channel.tryLockChannel()) {
          return new State(ChannelState.LOCKED_CHANNEL, i);
        }
      }
    }

    if (hasNonEmptyQueue) {
      // If there are non-empty queues but no unlocked channels, return a state indicating that.
      return new State(ChannelState.NO_UNLOCKED_CHANNEL, queueId);
    } else {
      return new State(ChannelState.NO_DATA);
    }
  }

  /**
   * Attempts to wait for the channel to become available for sending data.
   * If the channel is full, it adds the current waker to the channel's waiting list.
   *
   * @param channel The channel to wait on.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  void tryWait(Channel<T> channel) throws InterruptedException {
    boolean toWait;
    try {
      channel.getChannelGate().lockGate();
      toWait = channel.getChannelGate().addSenderWaker(currentWaker, channel.getId());
    } finally {
      channel.getChannelGate().unlockGate();
    }

    if (toWait) {
      // If the sender is added to the waiting list, let it await.
      channel.unlockChannel();
      channelLocked = false;
      currentWaker.await();
      // Update the current waker after being woken up
      currentWaker = getWaker();

      channel.lockChannel();
      channelLocked = true;
    }
  }

  /**
   * Wakes up the receiver associated with the given channel if it was waiting.
   * If the channel was empty before, it decrements the empty channel number.
   *
   * @param channel The channel to wake up the receiver for.
   */
  void wakeReceiver(Channel<T> channel) {
    // If the channel was empty before, decrement the empty channel number.
    channel.getChannelGate().decrementEmptyChannelNumber();
    // If the channel was empty before, wake up the receiver if there is one waiting.
    Waker waker = channel.getCurrentWaker();
    channel.unlockChannel();
    channelLocked = false;

    if (waker != null) {
      waker.wake();
    }
  }

  /**
   * Processes the data in the given channel and queue.
   * It locks the channel, checks if it is closed, and if not, adds data from the queue to the channel.
   * If the channel was empty before adding data, it wakes up the receiver.
   *
   * @param channel The channel to process data in.
   * @param queue   The queue containing data to be sent.
   * @return true if the channel is closed, false otherwise.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  boolean processData(Channel<T> channel, LinkedList<T> queue) throws InterruptedException {
    try {
      if (!channelLocked) {
        channel.lockChannel();
        channelLocked = true;
      }

      // todo: better stop condition
      if (channel.isClosed()) {
        return true;
      }

      // Checks if this sender should be added into waiting list:
      // 1. All channels are full.
      // 2. The current channel reached the maximum queue size.
      if (channel.getChannelGate().getEmptyChannelNumber() == 0 &&
              channel.isReachedMaxQueueSize()) {
        tryWait(channel);
        // Check again if channel was closed while waiting
        if (channel.isClosed()) {
          return true;
        }
      }

      boolean wasEmpty = channel.isEmpty();

      for (T item : queue) {
        channel.addData(item);
      }
      queue.clear();

      if (wasEmpty) {
        wakeReceiver(channel);
      }
    } finally {
      if (channelLocked) {
        channel.unlockChannel();
      }
    }
    return false;
  }

  public Future<Optional<Throwable>> getFuture(ExecutorService executor) {

    return executor.submit(() -> {
      // Set the task context for the current thread
      TaskContext$.MODULE$.setTaskContext(taskContext);

      Channel<T> channel = null;
      try {
        // Deserialize the task binary
        SerializerInstance ser = env.closureSerializer().newInstance();
        ClassTag<RDD<T>> tag = ClassTag$.MODULE$.apply(clazz);
        RDD<T> rdd = ser.deserialize(ByteBuffer.wrap(task),
                  Thread.currentThread().getContextClassLoader(), tag);
        Iterator<T> iterator = rdd.iterator(partition, taskContext);
        while (!Thread.currentThread().isInterrupted()) {
          boolean iterHasNext = iterator.hasNext();

          LinkedList<T> queue;

          if (iterHasNext) {
            T data = iterator.next();
            int key = partitioner.getPartition(data);
            channel = channels[key];

            // todo: better stop condition
            if (channel.isClosed()) {
              break;
            }

            queue = queues[key];
            queue.add(data);

            if (queue.size() < senderQueueSize) {
              continue;
            }

            if (queue.size() < senderQueueSize * QUEUE_FLUSH_THRESHOLD_MULTIPLIER) {
              if (channel.tryLockChannel()) {
                channelLocked = true;
              } else {
                continue;
              }
            } else {
              channelLocked = false;
            }
          } else {
            State state = nextQueue();
            if (state.isNoData()) {
              // No more data to send
              break;
            }

            int queueId = state.getChannelId();
            queue = queues[queueId];
            channel = channels[queueId];

            if (state.isLockedChannel()) {
              channelLocked = true;
            } else {
              channelLocked = false;
            }
          }

          if (processData(channel, queue)) {
            // If the channel is closed, break the loop
            break;
          }
        }

        sender.close();

        return Optional.empty();
      } catch (Throwable e) {
        // Set error on all channels to notify all receivers
        for (Channel<T> ch : channels) {
          ch.setError(e);
        }
        sender.close();
        return Optional.of(e);
      } finally {
        // Execute callback first, before cleanup, to chain next sender
        // Callback errors are logged but don't prevent cleanup
        if (callback != null) {
          try {
            callback.call();
          } catch (Exception e) {
            // Log callback error but continue with cleanup
            logger.error("Error in sender callback", e);
          }
        }

        taskContext.markTaskCompleted(Option.empty());

        // See `Task.scala` and `Executor.scala` for the details of the task lifecycle.
        try {
          env.blockManager().memoryStore().releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP,
                  Long.MAX_VALUE);
          env.blockManager().memoryStore().releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP,
                  Long.MAX_VALUE);
          MemoryManager memoryManager = env.blockManager().memoryManager();
          synchronized (memoryManager) {
            env.blockManager().memoryManager().notifyAll();
          }
        } finally {
          env.blockManager().releaseAllLocksForTask(taskContext.taskAttemptId());
          taskContext.taskMemoryManager().cleanUpAllAllocatedMemory();
          TaskContext$.MODULE$.unset();
        }
      }
    });
  }

  /**
   * Represents the state of channel availability when the sender needs to flush queued data.
   * Used by nextQueue() to determine which queue/channel pair to process.
   */
  enum ChannelState {
    /** No queues have pending data to send - sender is done */
    NO_DATA,
    /** Some queues have data but all corresponding channels are locked by other threads */
    NO_UNLOCKED_CHANNEL,
    /** Found a queue with data and successfully acquired its channel lock */
    LOCKED_CHANNEL,
  }

  /**
   * Encapsulates the result of attempting to find the next queue to process.
   * Contains the channel state and the channel ID if applicable.
   */
  class State {
    private final ChannelState state;
    private int channelId = -1;

    State(ChannelState state) {
      this.state = state;
    }

    State(ChannelState state, int channelId) {
      this.state = state;
      this.channelId = channelId;
    }

    boolean isNoData() {
      return state == ChannelState.NO_DATA;
    }

    boolean isLockedChannel() {
      return state == ChannelState.LOCKED_CHANNEL;
    }

    int getChannelId() {
      return channelId;
    }
  }
}
