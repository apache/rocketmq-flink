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

package org.apache.flink.connector.rocketmq.grpc.sink.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import java.io.Serializable;

/**
 * A serialization schema that converts a value of type {@code T} into a RocketMQ gRPC {@link
 * Message} using the given {@link MessageBuilder}.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface RocketMQGrpcSerializationSchema<T> extends Serializable {

    /**
     * Initialization method for the schema. It is called before the actual working method {@link
     * #serialize} and thus suitable for one time setup work.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {
        // Nothing to do here for the default implementation.
    }

    /**
     * Serialize the given element into a {@link Message}.
     *
     * @param element the element to serialize.
     * @param messageBuilder a fresh message builder to construct the message with.
     * @param timestamp the (nullable) event timestamp of the element.
     * @return the RocketMQ {@link Message} to send.
     */
    Message serialize(T element, MessageBuilder messageBuilder, Long timestamp);

    /**
     * Create a {@link RocketMQGrpcSerializationSchema} by wrapping a Flink {@link
     * SerializationSchema}. The value is serialized into the message body and published to the
     * given parent {@code topic} carrying the given {@code liteTopic}, so that a {@code
     * LiteSimpleConsumer} bound to the parent topic can receive it.
     */
    static <T> RocketMQGrpcSerializationSchema<T> flinkSchema(
            String topic, String liteTopic, SerializationSchema<T> serializationSchema) {
        return new RocketMQGrpcSerializationSchemaWrapper<>(topic, liteTopic, serializationSchema);
    }
}
