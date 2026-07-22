/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;

import java.time.Duration;

public class RocketMQSinkOptions extends RocketMQOptions {

    // rocketmq client API config prefix.
    public static final String PRODUCER_PREFIX = "rocketmq.sink.";

    public static final ConfigOption<String> PRODUCER_GROUP =
            ConfigOptions.key(PRODUCER_PREFIX + "group")
                    .stringType()
                    .defaultValue("PID-flink-producer");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key(PRODUCER_PREFIX + "topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the persist topic");

    public static final ConfigOption<String> SERIALIZE_FORMAT =
            ConfigOptions.key(PRODUCER_PREFIX + "serialize.format")
                    .stringType()
                    .defaultValue("json");

    public static final ConfigOption<String> DELIVERY_GUARANTEE =
            ConfigOptions.key(PRODUCER_PREFIX + "delivery.guarantee")
                    .stringType()
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE.name())
                    .withDescription("Optional delivery guarantee when committing.");

    public static final ConfigOption<Long> TRANSACTION_TIMEOUT =
            ConfigOptions.key(PRODUCER_PREFIX + "transaction.timeout")
                    .longType()
                    .defaultValue(Duration.ofMinutes(15).getSeconds());

    public static final ConfigOption<Integer> SEND_RETRY_TIMES =
            ConfigOptions.key(PRODUCER_PREFIX + "send.retry.times").intType().defaultValue(3);

    public static final ConfigOption<Integer> SEND_PENDING_MAX =
            ConfigOptions.key(PRODUCER_PREFIX + "send.pending.max")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of pending messages in the send buffer.");

    public static final ConfigOption<Long> SEND_TIMEOUT =
            ConfigOptions.key(PRODUCER_PREFIX + "send.timeout")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Message send timeout in ms.");

    public static final ConfigOption<Integer> EXECUTOR_NUM =
            ConfigOptions.key(PRODUCER_PREFIX + "executor.num")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Number of executor threads for sending messages.");

    public static final ConfigOption<String> TAG =
            ConfigOptions.key(PRODUCER_PREFIX + "tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Message tag for filtering.");

    // old config
    public static final ConfigOption<Integer> OPTIONAL_WRITE_RETRY_TIMES =
            ConfigOptions.key("rocketmq.sink.retry.times").intType().defaultValue(10);

    public static final ConfigOption<Long> OPTIONAL_WRITE_SLEEP_TIME_MS =
            ConfigOptions.key("rocketmq.sink.retry.sleep.ms").longType().defaultValue(5000L);

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_TAG =
            ConfigOptions.key("rocketmq.sink.tag.dynamic.enable").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN =
            ConfigOptions.key("rocketmq.sink.tag.dynamic.column").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED =
            ConfigOptions.key("rocketmq.sink.tag.dynamic.write.included").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_WRITE_KEY_COLUMNS =
            ConfigOptions.key("rocketmq.sink.key.columns").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_KEYS_TO_BODY =
            ConfigOptions.key("rocketmq.sink.key.write.to.body").booleanType().defaultValue(false);
}
