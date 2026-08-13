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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options for the RocketMQ gRPC {@code Producer} based sink. These options are
 * combined with the shared {@link org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions}.
 * The sink provides at-least-once semantics via synchronous {@code Producer.send()}.
 *
 * <p>These are the programmatic/SDK-facing options used by the sink builder and its runtime; every
 * key carries the {@link #PRODUCER_CONFIG_PREFIX} prefix. The SQL DDL keys are defined separately
 * in {@code RocketMQGrpcConnectorOptions}.
 */
@PublicEvolving
public class RocketMQGrpcSinkOptions {

    private RocketMQGrpcSinkOptions() {}

    /** Prefix for the RocketMQ gRPC sink options. */
    public static final String PRODUCER_CONFIG_PREFIX = "rocketmq.sink.";

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The parent topic to send records to when using a value-only "
                                    + "serializer.");

    public static final ConfigOption<String> LITE_TOPIC =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "lite-topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The lite (sub) topic to attach to every record when using a "
                                    + "value-only serializer. The message is published to the "
                                    + "parent topic configured via '"
                                    + PRODUCER_CONFIG_PREFIX
                                    + "topic' and carries this lite topic, so that a "
                                    + "LiteSimpleConsumer bound to the parent topic can receive "
                                    + "it.");

    public static final ConfigOption<Integer> MAX_ATTEMPTS =
            ConfigOptions.key(PRODUCER_CONFIG_PREFIX + "max-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The maximum number of send attempts for a message.");
}
