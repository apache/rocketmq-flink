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

package org.apache.flink.connector.rocketmq.grpc.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Config options that are used to configure the RocketMQ gRPC SQL connector. These keys are the
 * user-facing option names in a {@code CREATE TABLE ... WITH (...)} statement.
 *
 * <p>Connection/identity options are top-level (mirroring the {@code service-url}/{@code topics}
 * convention of the Pulsar and Kafka SQL connectors); functional options are grouped by role under
 * the {@code source.} and {@code sink.} prefixes. The programmatic/SDK-facing options that the
 * source and sink builders and their runtime consume are defined separately in {@link
 * org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions}, {@link
 * org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions} and {@link
 * org.apache.flink.connector.rocketmq.grpc.sink.RocketMQGrpcSinkOptions}.
 */
@PublicEvolving
public class RocketMQGrpcConnectorOptions {

    private RocketMQGrpcConnectorOptions() {}

    // --------------------------------------------------------------------------------------------
    // Connection options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> ENDPOINTS =
            ConfigOptions.key("endpoints")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The access point (proxy) endpoints the gRPC SDK communicates with, "
                                    + "for example '127.0.0.1:8080'.");

    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key("namespace")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The resource namespace of the RocketMQ instance.");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key("access-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access key used for static session credentials.");

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key("secret-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret key used for static session credentials.");

    public static final ConfigOption<String> CREDENTIALS_RESOLVER_CLASS =
            ConfigOptions.key("credentials-resolver-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The fully qualified class name of a CredentialsResolver that resolves "
                                    + "the session credentials per endpoint on the TaskManager. "
                                    + "When set, it takes precedence over 'access-key' and "
                                    + "'secret-key'.");

    public static final ConfigOption<Boolean> TLS_ENABLED =
            ConfigOptions.key("tls-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether TLS is enabled for the gRPC transport.");

    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(3))
                    .withDescription("The request timeout for a single gRPC invocation.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The default topic to send records to when the table is used as a "
                                    + "sink with a value-only serializer.");

    public static final ConfigOption<String> LITE_TOPIC =
            ConfigOptions.key("sink.lite-topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The lite (sub) topic attached to every record when the table is used "
                                    + "as a sink. The message is published to the parent topic "
                                    + "configured via 'topic' and carries this lite topic, so that "
                                    + "a LiteSimpleConsumer bound to the parent topic can receive "
                                    + "it. Required for sinks.");

    // --------------------------------------------------------------------------------------------
    // Source options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("source.consumer-group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The consumer group of the SimpleConsumer.");

    public static final ConfigOption<Duration> AWAIT_DURATION =
            ConfigOptions.key("source.await-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The long-polling await duration of a single receive() invocation.");

    public static final ConfigOption<Duration> INVISIBLE_DURATION =
            ConfigOptions.key("source.invisible-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "The invisible duration of a received message. It must be larger than "
                                    + "the checkpoint interval plus the checkpoint timeout so that "
                                    + "un-acked messages are redelivered after a failure.");

    public static final ConfigOption<Integer> MAX_MESSAGE_NUM =
            ConfigOptions.key("source.max-message-num")
                    .intType()
                    .defaultValue(32)
                    .withDescription("The maximum number of messages returned by receive().");

    public static final ConfigOption<Integer> FETCH_CONCURRENCY =
            ConfigOptions.key("source.fetch-concurrency")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The fetch concurrency of each subtask, i.e. the number of concurrent "
                                    + "fetch requests it issues. The requests are served by worker "
                                    + "threads sharing a single thread-safe SimpleConsumer, so "
                                    + "increasing this raises a single subtask's fetch throughput "
                                    + "without changing the Flink parallelism.");

    public static final ConfigOption<String> RENEWAL_POLICY_CLASS =
            ConfigOptions.key("source.renewal-policy-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The fully qualified class name of an InvisibleDurationRenewalPolicy "
                                    + "implementation consulted shortly before a still-buffered "
                                    + "message would become visible again; the policy decides "
                                    + "whether to extend its invisible duration. When absent, no "
                                    + "renewal is performed.");

    public static final ConfigOption<Duration> RENEWAL_AHEAD_TIME =
            ConfigOptions.key("source.renewal-ahead-time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "How long before a buffered message becomes visible again the renewal "
                                    + "policy is consulted. Must be positive and smaller than the "
                                    + "invisible duration. Only effective when "
                                    + "'source.renewal-policy-class' is configured.");

    // --------------------------------------------------------------------------------------------
    // Sink options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Integer> MAX_ATTEMPTS =
            ConfigOptions.key("sink.max-attempts")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The maximum number of send attempts for a message.");
}
