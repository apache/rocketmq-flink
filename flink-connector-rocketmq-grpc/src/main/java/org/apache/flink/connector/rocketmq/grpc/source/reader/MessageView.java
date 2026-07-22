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

import java.util.Collection;
import java.util.Map;

/**
 * A read-only view over a RocketMQ message returned by the gRPC {@code SimpleConsumer}. It exposes
 * the message attributes needed for deserialization while hiding the SDK receipt handle used for
 * acknowledgement.
 */
public interface MessageView {

    /** Get the unique message ID. */
    String getMessageId();

    /** Get the topic that the message belongs to. */
    String getTopic();

    /** Get the tag of the message, or {@code null} if the message has no tag. */
    String getTag();

    /** Get the keys of the message. */
    Collection<String> getKeys();

    /** Get the body of the message. */
    byte[] getBody();

    /** Get the number of times the message has been delivered. */
    int getDeliveryAttempt();

    /** Get the born timestamp of the message, used as the event time. */
    long getEventTime();

    /** Get the user-defined properties of the message. */
    Map<String, String> getProperties();
}
