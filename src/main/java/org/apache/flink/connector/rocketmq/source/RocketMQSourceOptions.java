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

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQConfigValidator;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategyFactory;

/** Includes config options of RocketMQ connector type. */
public class RocketMQSourceOptions extends RocketMQOptions {

    public static final RocketMQConfigValidator SOURCE_CONFIG_VALIDATOR =
            RocketMQConfigValidator.builder().build();

    // rocketmq source connector config prefix.
    public static final String CONSUMER_PREFIX = "rocketmq.source.";

    public static final String TOPIC_SEPARATOR = ";";

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key(CONSUMER_PREFIX + "group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the consumer group, used to identify a type of consumer");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key(CONSUMER_PREFIX + "topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the subscribe topic");

    public static final ConfigOption<Boolean> OPTIONAL_USE_NEW_API =
            ConfigOptions.key(CONSUMER_PREFIX + "api.new.enable").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_TAG =
            ConfigOptions.key(CONSUMER_PREFIX + "filter.tag")
                    .stringType()
                    .defaultValue("*")
                    .withDescription(
                            "for message filter, rocketmq only support single filter option");

    public static final ConfigOption<String> OPTIONAL_SQL =
            ConfigOptions.key(CONSUMER_PREFIX + "filter.sql").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_STARTUP_SCAN_MODE =
            ConfigOptions.key(CONSUMER_PREFIX + "startup.scan.mode")
                    .stringType()
                    .defaultValue("latest");

    /** for initialization consume offset */
    public static final ConfigOption<Long> OPTIONAL_STARTUP_OFFSET_SPECIFIC =
            ConfigOptions.key(CONSUMER_PREFIX + "startup.offset.specific")
                    .longType()
                    .defaultValue(RocketMQConfig.DEFAULT_START_MESSAGE_OFFSET);

    public static final ConfigOption<String> OPTIONAL_STARTUP_OFFSET_STRATEGY =
            ConfigOptions.key(CONSUMER_PREFIX + "startup.offset.strategy")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_STARTUP_OFFSET_DATE =
            ConfigOptions.key(CONSUMER_PREFIX + "startup.offset.date")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Long> OPTIONAL_STARTUP_OFFSET_TIMESTAMP =
            ConfigOptions.key(CONSUMER_PREFIX + "startup.offset.timestamp")
                    .longType()
                    .defaultValue(RocketMQConfig.DEFAULT_START_MESSAGE_OFFSET);

    public static final ConfigOption<String> OPTIONAL_STOP_OFFSET_TIMESTAMP =
            ConfigOptions.key(CONSUMER_PREFIX + "stop.offset.timestamp")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG =
            ConfigOptions.key(CONSUMER_PREFIX + "column.error.debug")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("If object deserialize failed, would print error message");

    public static final ConfigOption<String> ALLOCATE_MESSAGE_QUEUE_STRATEGY =
            ConfigOptions.key(CONSUMER_PREFIX + "allocate.strategy")
                    .stringType()
                    .defaultValue(AllocateStrategyFactory.STRATEGY_NAME_CONSISTENT_HASH)
                    .withDescription("The load balancing strategy algorithm");

    // pull message limit
    public static final ConfigOption<Integer> PULL_THREADS_NUM =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.threads.num")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The number of pull threads set");

    public static final ConfigOption<Long> PULL_BATCH_SIZE =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.batch.size")
                    .longType()
                    .defaultValue(32L)
                    .withDescription("The maximum number of messages pulled each time");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_QUEUE =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.threshold.queue")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The queue level flow control threshold");

    public static final ConfigOption<Long> PULL_THRESHOLD_FOR_ALL =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.threshold.all")
                    .longType()
                    .defaultValue(10 * 1000L)
                    .withDescription("The threshold for flow control of consumed requests");

    public static final ConfigOption<Long> PULL_TIMEOUT_MILLIS =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.rpc.timeout")
                    .longType()
                    .defaultValue(20 * 1000L)
                    .withDescription("The polling timeout setting");

    public static final ConfigOption<Long> PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.rpc.exception.delay")
                    .longType()
                    .defaultValue(3 * 1000L)
                    .withDescription(
                            "The maximum time that a connection will be suspended "
                                    + "for in long polling by the broker");

    public static final ConfigOption<Long> PULL_TIMEOUT_LONG_POLLING_SUSPEND =
            ConfigOptions.key(CONSUMER_PREFIX + "pull.suspend.timeout")
                    .longType()
                    .defaultValue(30 * 1000L)
                    .withDescription(
                            "The maximum wait time for a response from the broker "
                                    + "in long polling by the client");

    /** for auto commit offset to rocketmq server */
    public static final ConfigOption<Boolean> AUTO_COMMIT_OFFSET =
            ConfigOptions.key(CONSUMER_PREFIX + "offset.commit.auto")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The setting for automatic commit of offset");

    public static final ConfigOption<Long> AUTO_COMMIT_OFFSET_INTERVAL =
            ConfigOptions.key(CONSUMER_PREFIX + "offset.commit.interval")
                    .longType()
                    .defaultValue(5 * 1000L)
                    .withDescription(
                            "Applies to Consumer, the interval for persisting consumption progress");

    public static final ConfigOption<Boolean> COMMIT_OFFSETS_ON_CHECKPOINT =
            ConfigOptions.key(CONSUMER_PREFIX + "offset.commit.checkpoint")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to commit consuming offset on checkpoint.");

    /** for message trace, suggest not enable when heavy traffic */
    public static final ConfigOption<Boolean> ENABLE_MESSAGE_TRACE =
            ConfigOptions.key(CONSUMER_PREFIX + "trace.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("The flag for message tracing");

    public static final ConfigOption<String> CUSTOMIZED_TRACE_TOPIC =
            ConfigOptions.key(CONSUMER_PREFIX + "trace.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the topic for message tracing");
}
