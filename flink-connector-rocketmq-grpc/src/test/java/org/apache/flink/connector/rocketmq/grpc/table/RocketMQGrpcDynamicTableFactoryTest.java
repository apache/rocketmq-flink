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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.utils.FactoryMocks;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link RocketMQGrpcDynamicTableFactory}. */
class RocketMQGrpcDynamicTableFactoryTest {

    private static final String ACCESS_KEY_VALUE = "AK-TEST-VALUE";
    private static final String SECRET_KEY_VALUE = "SK-TEST-VALUE";

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(Column.physical("f0", DataTypes.STRING()));

    private static Map<String, String> baseOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", RocketMQGrpcDynamicTableFactory.IDENTIFIER);
        options.put("endpoints", "127.0.0.1:8080");
        options.put("topic", "test-topic");
        options.put("sink.lite-topic", "test-lite-topic");
        options.put("format", "test-format");
        options.put("test-format.delimiter", ",");
        return options;
    }

    @Test
    void createTableSourceTest() {
        // No 'source.mode' is configured, so the source runs in the default 'simple' mode.
        final Map<String, String> options = baseOptions();
        options.put("source.consumer-group", "test-group");

        final DynamicTableSource source = FactoryMocks.createTableSource(SCHEMA, options);
        assertThat(source).isInstanceOf(RocketMQGrpcDynamicTableSource.class);
    }

    @Test
    void createLiteModeTableSourceFailsTest() {
        final Map<String, String> options = baseOptions();
        options.put("source.consumer-group", "test-group");
        options.put("source.mode", "lite");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "lite' is not supported by the RocketMQ gRPC table source");
    }

    @Test
    void createTableSinkTest() {
        final DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, baseOptions());
        assertThat(sink).isInstanceOf(RocketMQGrpcDynamicTableSink.class);
    }

    @Test
    void createTableSourceFailsWithoutConsumerGroupTest() {
        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, baseOptions()))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("consumer-group");
    }

    @Test
    void createTableSinkFailsWithoutLiteTopicTest() {
        final Map<String, String> options = baseOptions();
        options.remove("sink.lite-topic");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("lite-topic");
    }

    @Test
    void createSimpleModeTableSourceTest() {
        final Map<String, String> options = baseOptions();
        options.put("source.consumer-group", "test-group");
        options.put("source.mode", "simple");
        options.put("source.filter-expression", "tagA||tagB");

        final DynamicTableSource source = FactoryMocks.createTableSource(SCHEMA, options);
        assertThat(source).isInstanceOf(RocketMQGrpcDynamicTableSource.class);
    }

    @Test
    void createTableSourceFailsWithInvalidModeTest() {
        final Map<String, String> options = baseOptions();
        options.put("source.consumer-group", "test-group");
        options.put("source.mode", "bogus");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .hasStackTraceContaining("bogus");
    }

    /**
     * A missing required option must be reported by its key only. Credentials set in the WITH
     * clause end up in the JobManager log and in the REST exception history, so the message the
     * connector produces must not repeat any option value.
     */
    @Test
    void missingSourceOptionIsReportedWithoutOptionValuesTest() {
        final Map<String, String> options = baseOptions();
        options.remove("endpoints");
        options.put("source.consumer-group", "test-group");
        options.put("access-key", ACCESS_KEY_VALUE);
        options.put("secret-key", SECRET_KEY_VALUE);

        final Throwable thrown =
                catchThrowable(() -> FactoryMocks.createTableSource(SCHEMA, options));

        assertThat(thrown).isInstanceOf(ValidationException.class);
        // Flink wraps every factory failure and lists the table options itself; assert on the
        // message the connector produced.
        final Throwable cause = thrown.getCause();
        assertThat(cause).isInstanceOf(ValidationException.class);
        assertThat(cause.getMessage())
                .startsWith("Missing required option")
                .contains("endpoints")
                .doesNotContain(ACCESS_KEY_VALUE)
                .doesNotContain(SECRET_KEY_VALUE);
    }

    @Test
    void missingSinkOptionIsReportedWithoutOptionValuesTest() {
        final Map<String, String> options = baseOptions();
        options.remove("topic");
        options.put("access-key", ACCESS_KEY_VALUE);
        options.put("secret-key", SECRET_KEY_VALUE);

        final Throwable thrown =
                catchThrowable(() -> FactoryMocks.createTableSink(SCHEMA, options));

        assertThat(thrown).isInstanceOf(ValidationException.class);
        final Throwable cause = thrown.getCause();
        assertThat(cause).isInstanceOf(ValidationException.class);
        assertThat(cause.getMessage())
                .startsWith("Missing required option")
                .contains("topic")
                .doesNotContain(ACCESS_KEY_VALUE)
                .doesNotContain(SECRET_KEY_VALUE);
    }
}
