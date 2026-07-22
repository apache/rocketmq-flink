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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.connector.rocketmq.source.reader.MessageView;

import org.apache.rocketmq.common.message.MessageQueue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface InnerConsumer extends AutoCloseable {

    /** start inner consumer */
    void start();

    /** Get the consumer group of the consumer. */
    String getConsumerGroup();

    /**
     * Fetch message queues of the topic.
     *
     * @param topic topic list
     * @return key is topic, values are message queue collections
     */
    CompletableFuture<Collection<MessageQueue>> fetchMessageQueues(String topic);

    /**
     * Manually assign a list of message queues to this consumer. This interface does not allow for
     * incremental assignment and will replace the previous assignment (if there is one).
     *
     * @param messageQueues Message queues that needs to be assigned.
     */
    void assign(Collection<MessageQueue> messageQueues);

    /**
     * Returns a set of message queues that are assigned to the current consumer. The assignment is
     * typically performed by a message broker and may change dynamically based on various factors
     * such as load balancing and consumer group membership.
     *
     * @return A set of message queues that are currently assigned to the consumer.
     */
    Set<MessageQueue> assignment();

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @return list of message, can be null.
     */
    List<MessageView> poll(Duration timeout);

    /** interrupt poll message */
    void wakeup();

    /**
     * Suspending message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be suspended.
     */
    void pause(Collection<MessageQueue> messageQueues);

    /**
     * Resuming message pulling from the message queues.
     *
     * @param messageQueues message queues that need to be resumed.
     */
    void resume(Collection<MessageQueue> messageQueues);

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this method is
     * invoked for the same message queue more than once, the latest offset will be used on the next
     * {@link #poll(Duration)}.
     *
     * @param messageQueue the message queue to override the fetch offset.
     * @param offset message offset.
     */
    void seek(MessageQueue messageQueue, long offset);

    /**
     * Seek consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    CompletableFuture<Long /*offset*/> seekCommittedOffset(MessageQueue messageQueue);

    /**
     * Seek consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    CompletableFuture<Long /*offset*/> seekMinOffset(MessageQueue messageQueue);

    /**
     * Seek consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    CompletableFuture<Long /*offset*/> seekMaxOffset(MessageQueue messageQueue);

    /**
     * Seek consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    CompletableFuture<Long /*offset*/> seekOffsetByTimestamp(
            MessageQueue messageQueue, long timestamp);

    /**
     * Seek consumer group previously committed offset
     *
     * @param messageQueue rocketmq queue to locate single queue
     * @return offset for message queue
     */
    CompletableFuture<Void> commitOffset(MessageQueue messageQueue, long offset);
}
