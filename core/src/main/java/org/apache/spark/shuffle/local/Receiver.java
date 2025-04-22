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

import java.util.concurrent.atomic.AtomicInteger;

public class Receiver<T> {
    private final Channel<T> channel;
    private boolean closed = false;
    private final int rddId;
    private int receiverId;
    private final AtomicInteger received = new AtomicInteger(0);

    private static AtomicInteger nextReceiverId = new AtomicInteger(0);

    Receiver(Channel<T> channel, int rddId) {
        this.channel  = channel;
        this.rddId = rddId;
        this.receiverId = nextReceiverId.getAndIncrement();
    }

    public ReceiverFuture<T> recv() {
        return new ReceiverFuture<>(receiverId, channel, rddId, received);
    }

    public Channel<T> getChannel() {
        return channel;
    }

    public void close() {
        if (closed) {
            return;
        }
        if (!channel.isClosed()) {
            System.out.println("Receiver " + receiverId + " (rdd: " + rddId + ") close channel " + channel.getId() + ", queue size:" + channel.getQueueSize() + ", wake senders: " + channel.getNumSenders() + ", received: " + received.get());
            channel.close();
            System.out.println("Receiver " + receiverId + " (rdd: " + rddId + ") closed channel " + channel.getId() + ", queue size:" + channel.getQueueSize() + ", wake senders: " + channel.getNumSenders() + ", received: " + received.get());
        }
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
