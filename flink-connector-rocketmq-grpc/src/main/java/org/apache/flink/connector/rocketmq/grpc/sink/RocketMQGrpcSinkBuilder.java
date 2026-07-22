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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.connector.rocketmq.grpc.sink.serialization.RocketMQGrpcSerializationSchema;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A fluent builder to construct a {@link RocketMQGrpcSink}. */
@PublicEvolving
public class RocketMQGrpcSinkBuilder<IN> {

    private final Configuration configuration;
    private RocketMQGrpcSerializationSchema<IN> serializationSchema;

    public RocketMQGrpcSinkBuilder() {
        this.configuration = new Configuration();
    }

    /** Set the gRPC access point (proxy) endpoints. */
    public RocketMQGrpcSinkBuilder<IN> setEndpoints(String endpoints) {
        return setConfig(RocketMQGrpcOptions.ENDPOINTS, endpoints);
    }

    /** Set the parent topic for value-only serialization. */
    public RocketMQGrpcSinkBuilder<IN> setTopic(String topic) {
        return setConfig(RocketMQGrpcSinkOptions.TOPIC, topic);
    }

    /** Set the lite (sub) topic attached to every record for value-only serialization. */
    public RocketMQGrpcSinkBuilder<IN> setLiteTopic(String liteTopic) {
        return setConfig(RocketMQGrpcSinkOptions.LITE_TOPIC, liteTopic);
    }

    /** Set the {@link RocketMQGrpcSerializationSchema}. */
    public RocketMQGrpcSinkBuilder<IN> setSerializer(
            RocketMQGrpcSerializationSchema<IN> serializationSchema) {
        this.serializationSchema = checkNotNull(serializationSchema);
        return this;
    }

    /**
     * Set a value-only serializer that encodes the record into the message body and publishes it to
     * the configured parent {@link RocketMQGrpcSinkOptions#TOPIC} carrying the configured {@link
     * RocketMQGrpcSinkOptions#LITE_TOPIC}.
     */
    public RocketMQGrpcSinkBuilder<IN> setValueOnlySerializer(
            SerializationSchema<IN> serializationSchema) {
        final String topic = configuration.get(RocketMQGrpcSinkOptions.TOPIC);
        checkNotNull(topic, "topic must be configured before setting a value-only serializer");
        final String liteTopic = configuration.get(RocketMQGrpcSinkOptions.LITE_TOPIC);
        checkNotNull(
                liteTopic, "lite topic must be configured before setting a value-only serializer");
        this.serializationSchema =
                RocketMQGrpcSerializationSchema.flinkSchema(topic, liteTopic, serializationSchema);
        return this;
    }

    /** Set an arbitrary configuration option. */
    public <T> RocketMQGrpcSinkBuilder<IN> setConfig(ConfigOption<T> key, T value) {
        configuration.set(key, value);
        return this;
    }

    /** Add arbitrary configuration options. */
    public RocketMQGrpcSinkBuilder<IN> setConfig(Configuration config) {
        configuration.addAll(config);
        return this;
    }

    /** Build the {@link RocketMQGrpcSink}. */
    public RocketMQGrpcSink<IN> build() {
        checkNotNull(
                configuration.get(RocketMQGrpcOptions.ENDPOINTS), "endpoints must be configured");
        checkNotNull(serializationSchema, "serializer must be configured");
        return new RocketMQGrpcSink<>(configuration, serializationSchema);
    }
}
