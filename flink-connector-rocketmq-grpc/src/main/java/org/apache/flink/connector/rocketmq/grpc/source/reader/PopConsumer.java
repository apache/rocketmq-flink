/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.grpc.source.reader;

import org.apache.flink.annotation.Internal;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;

/**
 * The minimal consumer surface the split reader needs. Both SDK consumers ({@code
 * LiteSimpleConsumer} and {@code SimpleConsumer}) expose the same Pop-model receive / ack /
 * change-invisible-duration operations but share no common interface, so the connector adapts
 * them to this single type.
 */
@Internal
public interface PopConsumer extends Closeable {

    /** Blocking long-polling receive. */
    List<MessageView> receive(int maxMessageNum, Duration invisibleDuration) throws ClientException;

    /** Acknowledge a received message, removing it from the Pop invisible set. */
    void ack(MessageView messageView) throws ClientException;

    /** Extend the invisible duration of a still-buffered message. */
    void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
            throws ClientException;
}
