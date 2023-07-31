/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source.reader;

import java.util.Collection;
import java.util.Map;

/** This interface defines the methods for obtaining information about a message in RocketMQ. */
public interface MessageView {

    /**
     * Get the unique message ID.
     *
     * @return the message ID
     */
    String getMessageId();

    /**
     * Get the topic that the message belongs to.
     *
     * @return the topic
     */
    String getTopic();

    /**
     * Get the name of the broker that handles the message.
     *
     * @return the broker name
     */
    String getBrokerName();

    /**
     * Get the ID of the queue that the message is stored in.
     *
     * @return the queue ID
     */
    int getQueueId();

    /**
     * Get the offset of the message within the queue.
     *
     * @return the queue offset
     */
    long getQueueOffset();

    /**
     * Get the tag of the message, which is used for filtering.
     *
     * @return the message tag
     */
    String getTag();

    /**
     * Get the keys of the message, which are used for partitioning and indexing.
     *
     * @return the message keys
     */
    Collection<String> getKeys();

    /**
     * Get the size of the message in bytes.
     *
     * @return the message size
     */
    int getStoreSize();

    /**
     * Get the body of the message.
     *
     * @return the message body
     */
    byte[] getBody();

    /**
     * Get the number of times that the message has been attempted to be delivered.
     *
     * @return the delivery attempt count
     */
    int getDeliveryAttempt();

    /**
     * Get the event time of the message, which is used for filtering and sorting.
     *
     * @return the event time
     */
    long getEventTime();

    /**
     * Get the ingestion time of the message, which is the time that the message was received by the
     * broker.
     *
     * @return the ingestion time
     */
    long getIngestionTime();

    /**
     * Get the properties of the message, which are set by the producer.
     *
     * @return the message properties
     */
    Map<String, String> getProperties();
}
