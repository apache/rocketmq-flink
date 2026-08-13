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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RocketMQGrpcSerializationSchema} that adapts a Flink {@link SerializationSchema} by
 * serializing the value into the message body and publishing it to a fixed parent topic carrying a
 * fixed lite (sub) topic.
 */
@Internal
public class RocketMQGrpcSerializationSchemaWrapper<T>
        implements RocketMQGrpcSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    private final String liteTopic;
    private final SerializationSchema<T> serializationSchema;

    public RocketMQGrpcSerializationSchemaWrapper(
            String topic, String liteTopic, SerializationSchema<T> serializationSchema) {
        this.topic = checkNotNull(topic, "topic must not be null");
        this.liteTopic = checkNotNull(liteTopic, "lite topic must not be null");
        this.serializationSchema = checkNotNull(serializationSchema);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        serializationSchema.open(context);
    }

    @Override
    public Message serialize(T element, MessageBuilder messageBuilder, Long timestamp) {
        return messageBuilder
                .setTopic(topic)
                .setLiteTopic(liteTopic)
                .setBody(serializationSchema.serialize(element))
                .build();
    }
}
