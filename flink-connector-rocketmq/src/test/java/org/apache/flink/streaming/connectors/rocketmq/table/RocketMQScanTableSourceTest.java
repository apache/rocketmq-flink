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

import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocketMQScanTableSource}. */
class RocketMQScanTableSourceTest {

    @Test
    void scanRuntimeProviderWithNewApiShouldReturnSourceProviderTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(
                        30000L, // pollTime
                        properties,
                        schema,
                        "test-topic", // topic
                        "test-group", // consumerGroup
                        "127.0.0.1:9876", // nameServerAddress
                        null, // accessKey
                        null, // secretKey
                        "*", // tag
                        null, // sql
                        Long.MAX_VALUE, // stopInMs (unbounded)
                        -1L, // startMessageOffset
                        -1L, // startTime
                        10000L, // partitionDiscoveryIntervalMs
                        "latest", // consumerOffsetMode
                        System.currentTimeMillis(), // consumerOffsetTimestamp
                        true); // useNewApi

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
    }

    @Test
    void scanRuntimeProviderWithNewApiAndBoundedShouldSucceedTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        long stopTime = System.currentTimeMillis() + 3600000L;

        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(
                        30000L,
                        properties,
                        schema,
                        "test-topic",
                        "test-group",
                        "127.0.0.1:9876",
                        null,
                        null,
                        "*",
                        null,
                        stopTime, // stopInMs (bounded)
                        -1L,
                        -1L,
                        10000L,
                        "latest",
                        System.currentTimeMillis(),
                        true);

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
    }

    @Test
    void scanRuntimeProviderWithNewApiAndEarliestOffsetShouldSucceedTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(
                        30000L,
                        properties,
                        schema,
                        "test-topic",
                        "test-group",
                        "127.0.0.1:9876",
                        null,
                        null,
                        "*",
                        null,
                        Long.MAX_VALUE,
                        -1L,
                        -1L,
                        10000L,
                        "earliest",
                        System.currentTimeMillis(),
                        true);

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
    }

    @Test
    void scanRuntimeProviderWithNewApiAndTimestampOffsetShouldSucceedTest() {
        DescriptorProperties properties = createBaseProperties();
        TableSchema schema = createTableSchema();

        long timestamp = 1700000000000L;

        RocketMQScanTableSource tableSource =
                new RocketMQScanTableSource(
                        30000L,
                        properties,
                        schema,
                        "test-topic",
                        "test-group",
                        "127.0.0.1:9876",
                        null,
                        null,
                        "*",
                        null,
                        Long.MAX_VALUE,
                        -1L,
                        timestamp,
                        10000L,
                        "timestamp",
                        timestamp,
                        true);

        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
    }

    private DescriptorProperties createBaseProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("connector", "rocketmq");
        props.put("rocketmq.source.topic", "test-topic");
        props.put("rocketmq.source.group", "test-group");
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
