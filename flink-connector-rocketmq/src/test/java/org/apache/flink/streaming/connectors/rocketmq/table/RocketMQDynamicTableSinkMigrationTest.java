/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rocketmq.table;

import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocketMQDynamicTableSink} FLIP-143 migration. */
class RocketMQDynamicTableSinkMigrationTest {

    @Test
    void sinkRuntimeProviderShouldReturnSinkV2ProviderTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        RocketMQDynamicTableSink tableSink =
                new RocketMQDynamicTableSink(
                        properties,
                        schema,
                        "test-topic",
                        "test-producer-group",
                        "127.0.0.1:9876",
                        null, // accessKey
                        null, // secretKey
                        null, // tag
                        null, // dynamicColumn
                        String.valueOf((char) 1), // fieldDelimiter (default SOH)
                        "UTF-8", // encoding
                        10, // retryTimes
                        5000L, // sleepTime
                        false, // isDynamicTag
                        true, // isDynamicTagIncluded
                        false, // writeKeysToBody
                        new String[0], // keyColumns
                        "AT_LEAST_ONCE", // deliveryGuarantee
                        3, // sendRetryTimes
                        5000L, // sendTimeout
                        1000, // sendPendingMax
                        4); // executorNum

        DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        SinkV2Provider sinkV2Provider = (SinkV2Provider) provider;
        assertThat(sinkV2Provider.createSink()).isInstanceOf(RocketMQSink.class);
    }

    @Test
    void sinkRuntimeProviderWithCredentialsShouldSucceedTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        RocketMQDynamicTableSink tableSink =
                new RocketMQDynamicTableSink(
                        properties,
                        schema,
                        "test-topic",
                        "test-producer-group",
                        "127.0.0.1:9876",
                        "myAccessKey", // accessKey
                        "mySecretKey", // secretKey
                        "testTag", // tag
                        null, // dynamicColumn
                        String.valueOf((char) 1), // fieldDelimiter
                        "UTF-8", // encoding
                        10, // retryTimes
                        5000L, // sleepTime
                        false, // isDynamicTag
                        true, // isDynamicTagIncluded
                        false, // writeKeysToBody
                        new String[0], // keyColumns
                        "AT_LEAST_ONCE", // deliveryGuarantee
                        3, // sendRetryTimes
                        5000L, // sendTimeout
                        1000, // sendPendingMax
                        4); // executorNum

        DynamicTableSink.SinkRuntimeProvider provider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
    }

    private DescriptorProperties createBaseProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "rocketmq");
        props.put("rocketmq.sink.topic", "test-topic");
        props.put("rocketmq.sink.group", "test-producer-group");
        props.put("rocketmq.client.endpoints", "127.0.0.1:9876");
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(props);
        descriptorProperties.putTableSchema("schema", createTableSchema());
        return descriptorProperties;
    }

    private TableSchema createTableSchema() {
        return TableSchema.builder()
                .field("id", DataTypes.BIGINT())
                .field("name", DataTypes.STRING())
                .build();
    }
}
