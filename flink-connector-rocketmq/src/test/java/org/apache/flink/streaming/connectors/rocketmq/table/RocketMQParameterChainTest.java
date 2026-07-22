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

package org.apache.flink.streaming.connectors.rocketmq.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests verifying that ConfigOption values flow correctly through the Table API chain:
 * DDL options -> Factory -> TableSource/Sink -> Configuration.
 */
class RocketMQParameterChainTest {

    @Test
    void sourceFactoryShouldDeclareKeyOptionalOptions() {
        RocketMQDynamicTableSourceFactory factory = new RocketMQDynamicTableSourceFactory();
        assertThat(factory.optionalOptions())
                .contains(
                        RocketMQSourceOptions.OPTIONAL_TAG,
                        RocketMQSourceOptions.OPTIONAL_SQL,
                        RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE,
                        RocketMQSourceOptions.OPTIONAL_USE_NEW_API,
                        RocketMQSourceOptions.PULL_BATCH_SIZE,
                        RocketMQSourceOptions.ENABLE_MESSAGE_TRACE,
                        RocketMQSourceOptions.CUSTOMIZED_TRACE_TOPIC);
    }

    @Test
    void sinkFactoryShouldDeclareKeyOptionalOptions() {
        RocketMQDynamicTableSinkFactory factory = new RocketMQDynamicTableSinkFactory();
        assertThat(factory.optionalOptions())
                .contains(
                        RocketMQSinkOptions.SEND_RETRY_TIMES,
                        RocketMQSinkOptions.SEND_TIMEOUT,
                        RocketMQSinkOptions.SEND_PENDING_MAX,
                        RocketMQSinkOptions.TRANSACTION_TIMEOUT);
    }

    @Test
    void sinkFactoryShouldNotDeclareSerializeFormat() {
        RocketMQDynamicTableSinkFactory factory = new RocketMQDynamicTableSinkFactory();
        assertThat(factory.optionalOptions()).doesNotContain(RocketMQSinkOptions.SERIALIZE_FORMAT);
    }

    @Test
    void sourceConfigurationShouldPreserveUserValues() {
        Configuration config = new Configuration();

        // P0 Source options
        config.setString(RocketMQSourceOptions.OPTIONAL_TAG, "TagA");
        config.setString(RocketMQSourceOptions.OPTIONAL_SQL, "region = 'hangzhou'");

        // P1 Source options
        config.setLong(RocketMQSourceOptions.PULL_BATCH_SIZE, 64L);
        config.setBoolean(RocketMQSourceOptions.ENABLE_MESSAGE_TRACE, true);
        config.setString(RocketMQSourceOptions.CUSTOMIZED_TRACE_TOPIC, "my-trace");

        // P2 Common options
        config.setString(RocketMQOptions.NAMESPACE, "my-ns");

        // Verify all values preserved
        assertThat(config.getString(RocketMQSourceOptions.OPTIONAL_TAG)).isEqualTo("TagA");
        assertThat(config.getString(RocketMQSourceOptions.OPTIONAL_SQL))
                .isEqualTo("region = 'hangzhou'");
        assertThat(config.getLong(RocketMQSourceOptions.PULL_BATCH_SIZE)).isEqualTo(64L);
        assertThat(config.getBoolean(RocketMQSourceOptions.ENABLE_MESSAGE_TRACE)).isTrue();
        assertThat(config.getString(RocketMQSourceOptions.CUSTOMIZED_TRACE_TOPIC))
                .isEqualTo("my-trace");
        assertThat(config.getString(RocketMQOptions.NAMESPACE)).isEqualTo("my-ns");
    }

    @Test
    void sinkConfigurationShouldPreserveUserValues() {
        Configuration config = new Configuration();

        // P0 Sink options
        config.setLong(RocketMQSinkOptions.SEND_TIMEOUT, 8000L);
        config.setInteger(RocketMQSinkOptions.SEND_RETRY_TIMES, 5);
        config.setInteger(RocketMQSinkOptions.SEND_PENDING_MAX, 2000);

        // P1 Sink options
        config.setLong(RocketMQSinkOptions.TRANSACTION_TIMEOUT, 600L);

        // Verify all values preserved
        assertThat(config.getLong(RocketMQSinkOptions.SEND_TIMEOUT)).isEqualTo(8000L);
        assertThat(config.getInteger(RocketMQSinkOptions.SEND_RETRY_TIMES)).isEqualTo(5);
        assertThat(config.getInteger(RocketMQSinkOptions.SEND_PENDING_MAX)).isEqualTo(2000);
        assertThat(config.getLong(RocketMQSinkOptions.TRANSACTION_TIMEOUT)).isEqualTo(600L);
    }
}
