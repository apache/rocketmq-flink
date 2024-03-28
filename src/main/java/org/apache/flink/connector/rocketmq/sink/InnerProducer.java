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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CompletableFuture;

/**
 * InnerProducer is an interface that represents a message producer used for sending messages to a
 * messaging system.
 *
 * @see AutoCloseable
 */
public interface InnerProducer extends AutoCloseable {

    /** Starts the inner consumer. */
    void start();

    /**
     * Gets the consumer group of the consumer.
     *
     * @return the consumer group of the consumer
     */
    String getProducerGroup();

    /**
     * Sends the message to the messaging system and returns a Future for the send operation.
     *
     * @param message the message to be sent
     * @return a Future for the send operation that returns a SendResult object
     * @see CompletableFuture
     * @see SendResult
     */
    CompletableFuture<SendResult> send(Message message);

    /**
     * Sends the message to the messaging system and returns a Future for the send operation.
     *
     * @param message the message to be sent
     * @return a Future for the send operation that returns a SendResult object
     * @see CompletableFuture
     * @see SendResult
     */
    CompletableFuture<SendResult> sendMessageInTransaction(Message message);

    /**
     * Commits the send operation identified by the specified SendCommittable object.
     *
     * @param sendCommittable the SendCommittable object identifying the send operation
     * @return a Future that indicates whether the commit operation was successful
     * @see CompletableFuture
     * @see SendCommittable
     */
    CompletableFuture<Void> commit(SendCommittable sendCommittable);

    /**
     * Rolls back the send operation identified by the specified SendCommittable object.
     *
     * @param sendCommittable the SendCommittable object identifying the send operation
     * @return a Future that indicates whether the rollback operation was successful
     * @see CompletableFuture
     * @see SendCommittable
     */
    CompletableFuture<Void> rollback(SendCommittable sendCommittable);
}
