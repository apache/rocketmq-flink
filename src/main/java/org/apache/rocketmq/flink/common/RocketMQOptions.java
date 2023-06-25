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

package org.apache.rocketmq.flink.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.DEFAULT_START_MESSAGE_OFFSET;

/** Includes config options of RocketMQ connector type. */
public class RocketMQOptions {

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic").stringType().noDefaultValue();

    public static final ConfigOption<String> CONSUMER_GROUP =
            ConfigOptions.key("consumerGroup").stringType().noDefaultValue();

    public static final ConfigOption<String> PRODUCER_GROUP =
            ConfigOptions.key("producerGroup").stringType().noDefaultValue();

    public static final ConfigOption<String> NAME_SERVER_ADDRESS =
            ConfigOptions.key("nameServerAddress").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_TAG =
            ConfigOptions.key("tag").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SQL =
            ConfigOptions.key("sql").stringType().noDefaultValue();

    public static final ConfigOption<Long> OPTIONAL_START_MESSAGE_OFFSET =
            ConfigOptions.key("startMessageOffset")
                    .longType()
                    .defaultValue(DEFAULT_START_MESSAGE_OFFSET);

    public static final ConfigOption<Long> OPTIONAL_START_TIME_MILLS =
            ConfigOptions.key("startTimeMs").longType().defaultValue(-1L);

    public static final ConfigOption<String> OPTIONAL_START_TIME =
            ConfigOptions.key("startTime").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_END_TIME =
            ConfigOptions.key("endTime").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_TIME_ZONE =
            ConfigOptions.key("timeZone").stringType().noDefaultValue();

    public static final ConfigOption<Long> OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key("partitionDiscoveryIntervalMs").longType().defaultValue(30000L);

    public static final ConfigOption<Long> OPTIONAL_CONSUMER_POLL_MS =
            ConfigOptions.key("consumer.timeout").longType().defaultValue(3000L);

    public static final ConfigOption<Boolean> OPTIONAL_USE_NEW_API =
            ConfigOptions.key("useNewApi").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_ENCODING =
            ConfigOptions.key("encoding").stringType().defaultValue("UTF-8");

    public static final ConfigOption<String> OPTIONAL_FIELD_DELIMITER =
            ConfigOptions.key("fieldDelimiter").stringType().defaultValue("\u0001");

    public static final ConfigOption<String> OPTIONAL_LINE_DELIMITER =
            ConfigOptions.key("lineDelimiter").stringType().defaultValue("\n");

    public static final ConfigOption<Boolean> OPTIONAL_COLUMN_ERROR_DEBUG =
            ConfigOptions.key("columnErrorDebug").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_LENGTH_CHECK =
            ConfigOptions.key("lengthCheck").stringType().defaultValue("NONE");

    public static final ConfigOption<Integer> OPTIONAL_WRITE_RETRY_TIMES =
            ConfigOptions.key("retryTimes").intType().defaultValue(10);

    public static final ConfigOption<Long> OPTIONAL_WRITE_SLEEP_TIME_MS =
            ConfigOptions.key("sleepTimeMs").longType().defaultValue(5000L);

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_IS_DYNAMIC_TAG =
            ConfigOptions.key("isDynamicTag").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN =
            ConfigOptions.key("dynamicTagColumn").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED =
            ConfigOptions.key("dynamicTagColumnWriteIncluded").booleanType().defaultValue(true);

    public static final ConfigOption<String> OPTIONAL_WRITE_KEY_COLUMNS =
            ConfigOptions.key("keyColumns").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> OPTIONAL_WRITE_KEYS_TO_BODY =
            ConfigOptions.key("writeKeysToBody").booleanType().defaultValue(false);

    public static final ConfigOption<String> OPTIONAL_ACCESS_KEY =
            ConfigOptions.key("accessKey").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SECRET_KEY =
            ConfigOptions.key("secretKey").stringType().noDefaultValue();

    public static final ConfigOption<String> OPTIONAL_SCAN_STARTUP_MODE =
            ConfigOptions.key("scanStartupMode").stringType().defaultValue("latest");

    public static final ConfigOption<Long> OPTIONAL_OFFSET_FROM_TIMESTAMP =
            ConfigOptions.key("offsetFromTimestamp").longType().noDefaultValue();

    // --------------------------------------------------------------------------------------------
    // Format options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    // .defaultValue("rocketmq-default")
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    public static final ConfigOption<ValueFieldsStrategy> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(ValueFieldsStrategy.class)
                    .defaultValue(ValueFieldsStrategy.ALL)
                    .withDescription(
                            String.format(
                                    "Defines a strategy how to deal with key columns in the data type "
                                            + "of the value format. By default, '%s' physical columns of the table schema "
                                            + "will be included in the value format which means that the key columns "
                                            + "appear in the data type for both the key and value format.",
                                    ValueFieldsStrategy.ALL));

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Defines a custom prefix for all fields of the key format to avoid "
                                                    + "name clashes with fields of the value format. "
                                                    + "By default, the prefix is empty.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "If a custom prefix is defined, both the table schema and '%s' will work with prefixed names.",
                                                    KEY_FIELDS.key()))
                                    .linebreak()
                                    .text(
                                            "When constructing the data type of the key format, the prefix "
                                                    + "will be removed and the non-prefixed names will be used within the key format.")
                                    .linebreak()
                                    .text(
                                            String.format(
                                                    "Please note that this option requires that '%s' must be '%s'.",
                                                    VALUE_FIELDS_INCLUDE.key(),
                                                    ValueFieldsStrategy.EXCEPT_KEY))
                                    .build());

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    public enum ValueFieldsStrategy {
        ALL,
        EXCEPT_KEY
    }
}
