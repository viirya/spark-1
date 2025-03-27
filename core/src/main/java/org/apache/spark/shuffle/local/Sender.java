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

public class Sender<T> {
    private final Channel<T> channel;
    private boolean closed = false;

    Sender(Channel<T> channel) {
          this.channel  = channel;
    }

    public Channel<T> getChannel() {
        return channel;
    }

    public void close() {
        if (!closed) {
            if (!channel.isClosed()) {
                if (channel.isEmpty() && channel.getNumSenders() == 1) {
                    channel.getChannelGate().decrementEmptyChannelNumber();
                }
            }

            channel.reduceNumSenders();
            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public SenderFuture<T> send(T data) {
        return new SenderFuture<>(data, this, channel);
    }
}
