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
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.sink.serialization.RocketMQGrpcSerializationSchema;

import java.io.IOException;

/**
 * The gRPC ({@code rocketmq-client-java}) implementation of a RocketMQ {@link Sink}. It provides
 * at-least-once semantics using the synchronous {@code Producer.send()}; there is no committer and
 * no two-phase transaction commit.
 */
@PublicEvolving
public class RocketMQGrpcSink<IN> implements Sink<IN> {

    private static final long serialVersionUID = 1L;

    private final Configuration configuration;
    private final RocketMQGrpcSerializationSchema<IN> serializationSchema;

    RocketMQGrpcSink(
            Configuration configuration, RocketMQGrpcSerializationSchema<IN> serializationSchema) {
        this.configuration = configuration;
        this.serializationSchema = serializationSchema;
    }

    /** Create a {@link RocketMQGrpcSinkBuilder} to construct a new {@link RocketMQGrpcSink}. */
    public static <IN> RocketMQGrpcSinkBuilder<IN> builder() {
        return new RocketMQGrpcSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return new RocketMQGrpcSinkWriter<>(
                configuration,
                serializationSchema,
                context.asSerializationSchemaInitializationContext());
    }

    @Deprecated
    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return new RocketMQGrpcSinkWriter<>(
                configuration,
                serializationSchema,
                context.asSerializationSchemaInitializationContext());
    }
}
