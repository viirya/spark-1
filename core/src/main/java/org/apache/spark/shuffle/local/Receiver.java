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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The receiver is responsible for receiving data from the given channel. It is also responsible for
 * closing the channel when it is no longer needed.
 *
 * @param <T> The type of the data to be received.
 */
public class Receiver<T> {
  private final Channel<T> channel;
  private boolean closed = false;
  private final int rddId;
  private int receiverId;
  private final AtomicInteger received = new AtomicInteger(0);

  private static final AtomicInteger nextReceiverId = new AtomicInteger(0);

  private final int maxQueueSize;
  private final LinkedList<T> queue = new LinkedList<>();

  Receiver(Channel<T> channel, int rddId, int maxQueueSize) {
    this.channel = channel;
    this.rddId = rddId;
    this.receiverId = nextReceiverId.getAndIncrement();
    this.maxQueueSize = maxQueueSize;
  }

  public ReceiverFuture<T> recv() {
    return new ReceiverFuture<>(receiverId, channel, rddId, received, queue, maxQueueSize);
  }

  public Channel<T> getChannel() {
    return channel;
  }

  public void close() {
    if (closed) {
      return;
    }
    try {
      channel.lockChannel();
      if (!channel.isClosed()) {
        // Close the channel and decrement the empty channel number if needed.
        channel.setClosed();

        if (channel.isEmpty() && channel.getNumSenders() > 0) {
          channel.getChannelGate().decrementEmptyChannelNumber();
        }
        channel.cleanUp();
        channel.getChannelGate().wakeSenders(channel.getId());
      }
    } finally {
      channel.unlockChannel();
    }
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }
}
