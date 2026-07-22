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

package org.apache.flink.connector.rocketmq.grpc.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.sink.serialization.RocketMQGrpcSerializationSchema;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;

import java.io.IOException;

/**
 * The {@link SinkWriter} that synchronously sends records to RocketMQ using the gRPC {@code
 * Producer}. Each {@code send()} is a synchronous call, providing at-least-once semantics.
 */
@Internal
public class RocketMQGrpcSinkWriter<IN> implements SinkWriter<IN> {

    private final ClientServiceProvider provider;
    private final Producer producer;
    private final RocketMQGrpcSerializationSchema<IN> serializationSchema;

    public RocketMQGrpcSinkWriter(
            Configuration configuration,
            RocketMQGrpcSerializationSchema<IN> serializationSchema,
            SerializationSchema.InitializationContext initializationContext)
            throws IOException {
        this.provider = ClientServiceProvider.loadService();
        this.serializationSchema = serializationSchema;
        try {
            this.serializationSchema.open(initializationContext);
            this.producer = ProducerProvider.create(provider, configuration);
        } catch (Exception e) {
            throw new IOException("Failed to initialize the RocketMQ gRPC producer.", e);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        final Long timestamp = context.timestamp();
        final Message message =
                serializationSchema.serialize(element, provider.newMessageBuilder(), timestamp);
        try {
            producer.send(message);
        } catch (ClientException e) {
            throw new IOException("Failed to send message to RocketMQ.", e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // Messages are sent synchronously, so there is nothing to flush.
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }
}
