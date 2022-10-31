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

package org.apache.rocketmq.flink.sink2.writer.serializer;

import org.apache.rocketmq.common.message.Message;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.Serializable;

/**
 * A serialization schema which defines how to convert a value of type {@code T} to {@link Message}.
 *
 * @param <T> the type of values being serialized
 */
public interface RocketMQMessageSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #serialize(Object, RocketMQSinkContext, Long)} and thus suitable for one time setup work.
     *
     * <p>The provided {@link SerializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     * @param sinkContext runtime information i.e. partitions, subtaskId
     */
    default void open(
            SerializationSchema.InitializationContext context, RocketMQSinkContext sinkContext)
            throws Exception {}

    /**
     * Serializes given element and returns it as a {@link Message}.
     *
     * @param element element to be serialized
     * @param context context to possibly determine target partition
     * @param timestamp timestamp
     * @return RocketMQ {@link Message}
     */
    Message serialize(T element, RocketMQSinkContext context, Long timestamp);

    /** Context providing information of the RocketMQ record target location. */
    @Internal
    interface RocketMQSinkContext {

        /**
         * Get the ID of the subtask the RocketMQSink is running on. The numbering starts from 0 and
         * goes up to parallelism-1. (parallelism as returned by {@link
         * #getNumberOfParallelInstances()}
         *
         * @return ID of subtask
         */
        int getParallelInstanceId();

        /** @return number of parallel RocketMQSink tasks. */
        int getNumberOfParallelInstances();

        /**
         * For a given topic id retrieve the available partitions.
         *
         * <p>After the first retrieval the returned partitions are cached. If the partitions are
         * updated the job has to be restarted to make the change visible.
         *
         * @param topic RocketMQ topic with partitions
         * @return the ids of the currently available partitions
         */
        int[] getPartitionsForTopic(String topic);
    }

    /**
     * Creates a default schema builder to provide common building blocks i.e. key serialization,
     * value serialization, partitioning.
     *
     * @param <T> type of incoming elements
     * @return {@link RocketMQMessageSerializationSchemaBuilder}
     */
    static <T> RocketMQMessageSerializationSchemaBuilder<T> builder() {
        return new RocketMQMessageSerializationSchemaBuilder<>();
    }
}
