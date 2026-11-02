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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for verifying tag/sql filter configuration in the new API path. */
class RocketMQScanTableSourceFilterTest {

    @Test
    void tagFilterShouldBePreservedOnCopy() {
        DescriptorProperties props = new DescriptorProperties();
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("name", DataTypes.STRING()));

        RocketMQScanTableSource source =
                new RocketMQScanTableSource(
                        10L,
                        props,
                        schema,
                        "test-topic",
                        "test-group",
                        "127.0.0.1:9876",
                        null,
                        null,
                        "TagA",
                        null,
                        Long.MAX_VALUE,
                        -1L,
                        -1L,
                        10000L,
                        "latest",
                        -1L);

        RocketMQScanTableSource copy = (RocketMQScanTableSource) source.copy();
        assertThat(copy).isNotNull();
        assertThat(copy.asSummaryString()).isEqualTo(source.asSummaryString());
    }

    @Test
    void sqlFilterShouldBePreservedOnCopy() {
        DescriptorProperties props = new DescriptorProperties();
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));

        RocketMQScanTableSource source =
                new RocketMQScanTableSource(
                        10L,
                        props,
                        schema,
                        "test-topic",
                        "test-group",
                        "127.0.0.1:9876",
                        null,
                        null,
                        "*",
                        "region = 'hangzhou'",
                        Long.MAX_VALUE,
                        -1L,
                        -1L,
                        10000L,
                        "latest",
                        -1L);

        RocketMQScanTableSource copy = (RocketMQScanTableSource) source.copy();
        assertThat(copy).isNotNull();
    }

    @Test
    void wildcardTagShouldMatchAllMessages() {
        // "*" is the default tag and should not filter any messages
        DescriptorProperties props = new DescriptorProperties();
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));

        RocketMQScanTableSource source =
                new RocketMQScanTableSource(
                        10L,
                        props,
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
                        "latest",
                        -1L);

        assertThat(source).isNotNull();
    }
}
