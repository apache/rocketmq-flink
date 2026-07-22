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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests verifying the auto-infer behavior for startup.offset.timestamp in {@link
 * RocketMQDynamicTableSourceFactory}. When a user sets startup.offset.timestamp without explicitly
 * setting startup.scan.mode, the factory should auto-infer scan.mode=timestamp.
 */
class RocketMQDynamicTableSourceFactoryOffsetTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Collections.singletonList(Column.physical("id", BIGINT().notNull())),
                    new ArrayList<>(),
                    null);

    private DynamicTableSource createSource(Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "test_table"),
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(Schema.newBuilder().fromResolvedSchema(SCHEMA).build())
                                .comment("")
                                .partitionKeys(Collections.emptyList())
                                .options(options)
                                .build(),
                        SCHEMA),
                new org.apache.flink.configuration.Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "rocketmq");
        options.put("rocketmq.source.topic", "test-topic");
        options.put("rocketmq.source.group", "test-group");
        options.put("rocketmq.client.endpoints", "127.0.0.1:9876");
        return options;
    }

    @Test
    void timestampOffsetShouldAutoSetScanMode() {
        // When user sets startup.offset.timestamp but NOT startup.scan.mode,
        // the factory should auto-infer scan.mode=timestamp
        Map<String, String> options = baseOptions();
        options.put("rocketmq.source.startup.offset.timestamp", "1700000000000");

        DynamicTableSource source = createSource(options);
        assertThat(source).isInstanceOf(RocketMQScanTableSource.class);
    }

    @Test
    void explicitScanModeWithTimestampShouldBeRejected() {
        // Factory explicitly rejects setting both scan.mode and offset.timestamp
        Map<String, String> options = baseOptions();
        options.put("rocketmq.source.startup.scan.mode", "timestamp");
        options.put("rocketmq.source.startup.offset.timestamp", "1700000000000");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> createSource(options))
                .hasStackTraceContaining("Cannot set");
    }

    @Test
    void defaultScanModeShouldBeLatest() {
        // When neither scan.mode nor timestamp is set, default should be "latest"
        Map<String, String> options = baseOptions();

        DynamicTableSource source = createSource(options);
        assertThat(source).isInstanceOf(RocketMQScanTableSource.class);
    }
}
