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

package org.apache.flink.connector.rocketmq.grpc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Configuration options for the RocketMQ gRPC {@code SimpleConsumer} based source. These options
 * are combined with the shared {@link
 * org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions}.
 *
 * <p>These are the programmatic/SDK-facing options used by the source builder and its runtime;
 * every key carries the {@link #CONSUMER_CONFIG_PREFIX} prefix. The SQL DDL keys are defined
 * separately in {@code RocketMQGrpcConnectorOptions}.
 */
@PublicEvolving
public class RocketMQGrpcSourceOptions {

    private RocketMQGrpcSourceOptions() {}

    /** Prefix for the RocketMQ gRPC source options. */
    public static final String CONSUMER_CONFIG_PREFIX = "rocketmq.source.";

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "consumer-group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The consumer group of the SimpleConsumer.");

    public static final ConfigOption<ConsumerMode> MODE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "mode")
                    .enumType(ConsumerMode.class)
                    .defaultValue(ConsumerMode.SIMPLE)
                    .withDescription(
                            "The consumption mode. SIMPLE (the default) subscribes to a normal "
                                    + "topic via a SimpleConsumer and the source acknowledges "
                                    + "messages when their checkpoint completes; LITE binds a main "
                                    + "lite topic via a LiteSimpleConsumer and defers "
                                    + "acknowledgement to a downstream operator.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The normal topic subscribed by the SimpleConsumer. Required in "
                                    + "SIMPLE mode; ignored in LITE mode.");

    public static final ConfigOption<String> FILTER_EXPRESSION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "filter-expression")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The filter expression applied to the SIMPLE mode subscription, e.g. "
                                    + "a tag expression 'tagA||tagB' or a SQL92 expression. When "
                                    + "absent, all messages are received. Only effective in "
                                    + "SIMPLE mode.");

    public static final ConfigOption<String> FILTER_TYPE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "filter-type")
                    .stringType()
                    .defaultValue("TAG")
                    .withDescription(
                            "The type of 'filter-expression': TAG or SQL92. Only effective in "
                                    + "SIMPLE mode.");

    public static final ConfigOption<String> MAIN_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "main-topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The main lite topic bound by the LiteSimpleConsumer. Required in "
                                    + "LITE mode; ignored in SIMPLE mode. Every subtask binds this "
                                    + "topic; the broker performs message-level load balancing "
                                    + "across the consumer group.");

    public static final ConfigOption<Integer> FETCH_CONCURRENCY =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "fetch-concurrency")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The fetch concurrency of each subtask, i.e. the number of concurrent "
                                    + "fetch requests it issues. The requests are served by worker "
                                    + "threads sharing a single thread-safe SimpleConsumer, so "
                                    + "increasing this raises a single subtask's fetch throughput "
                                    + "without changing the Flink parallelism.");

    public static final ConfigOption<Duration> AWAIT_DURATION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "await-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(20))
                    .withDescription(
                            "The long-polling await duration of a single receive() invocation.");

    public static final ConfigOption<Duration> INVISIBLE_DURATION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "invisible-duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription(
                            "The invisible duration of a received message; un-acked messages are "
                                    + "redelivered after it expires, which provides the "
                                    + "at-least-once guarantee. In LITE mode it must be larger than "
                                    + "the full downstream processing time of a message, because a "
                                    + "downstream operator acknowledges it. In SIMPLE mode the "
                                    + "source acknowledges on checkpoint completion, so size it "
                                    + "with headroom above two checkpoint intervals plus the "
                                    + "checkpoint timeout: a failed checkpoint defers the ack to "
                                    + "the next successful one.");

    public static final ConfigOption<Integer> MAX_MESSAGE_NUM =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "max-message-num")
                    .intType()
                    .defaultValue(32)
                    .withDescription("The maximum number of messages returned by receive().");

    public static final ConfigOption<String> RENEWAL_POLICY_CLASS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "renewal-policy-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The fully qualified class name of an InvisibleDurationRenewalPolicy "
                                    + "implementation. When configured, the source consults the "
                                    + "policy 'renewal-ahead-time' before a still-buffered message "
                                    + "would become visible again, and the policy decides whether "
                                    + "to extend its invisible duration. When absent, no renewal "
                                    + "is performed.");

    public static final ConfigOption<Duration> RENEWAL_AHEAD_TIME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "renewal-ahead-time")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription(
                            "How long before a buffered message becomes visible again the renewal "
                                    + "policy is consulted. For example with a 60s invisible "
                                    + "duration and a 5s ahead time the policy runs 55s after the "
                                    + "message was received. Must be positive and smaller than "
                                    + "the invisible duration. Only effective when "
                                    + "'renewal-policy-class' is configured.");
}
