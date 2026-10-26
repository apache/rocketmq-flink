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

package org.apache.flink.connector.rocketmq.source.reader.deserializer;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the format options a user writes in the DDL actually reach {@link
 * RowDeserializationSchema}. Reading them under a different key than the one the table factory
 * registers makes every option silently ineffective, and with a multi column schema the default
 * field delimiter then drops every record instead of failing.
 */
class RowDeserializationSchemaOptionsTest {

    private static final String FIELD_DELIMITER_KEY = "rocketmq.client.message.field.delimiter";
    private static final String LENGTH_CHECK_KEY = "rocketmq.client.message.length.check";

    @Test
    void fieldDelimiterFromDdlSplitsRowTest() {
        List<RowData> rows = deserialize(options(FIELD_DELIMITER_KEY, "|"), "SMOKE1|a|b|c|d|e");

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getArity()).isEqualTo(6);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("SMOKE1");
        assertThat(rows.get(0).getString(5).toString()).isEqualTo("e");
    }

    @Test
    void defaultDelimiterDoesNotSplitPipeSeparatedBodyTest() {
        // Documents the trap the option above avoids: with the default delimiter the body is a
        // single field, so a six column row has too few fields and is dropped.
        assertThat(deserialize(ddlOptions(), "SMOKE1|a|b|c|d|e")).isEmpty();
    }

    @Test
    void lineDelimiterFromDdlSplitsBodyIntoRowsTest() {
        Map<String, String> options = options(FIELD_DELIMITER_KEY, "|");
        options.put("rocketmq.client.message.line.delimiter", "#");

        List<RowData> rows = deserialize(options, "SMOKE1|a|b|c|d|e#SMOKE1|f|g|h|i|j");

        assertThat(rows).hasSize(2);
        assertThat(rows.get(1).getString(1).toString()).isEqualTo("f");
    }

    @Test
    void unsupportedLengthCheckIsRejectedTest() {
        assertThatThrownBy(() -> deserialize(options(LENGTH_CHECK_KEY, "BOGUS"), "SMOKE1|a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(LENGTH_CHECK_KEY)
                .hasMessageContaining("BOGUS");
    }

    @Test
    void lengthCheckExceptionFailsInsteadOfSkippingTest() {
        Map<String, String> options = options(FIELD_DELIMITER_KEY, "|");
        options.put(LENGTH_CHECK_KEY, "EXCEPTION");

        assertThatThrownBy(() -> deserialize(options, "only-one-field"))
                .isInstanceOf(RuntimeException.class);
    }

    private static Map<String, String> options(String key, String value) {
        Map<String, String> options = ddlOptions();
        options.put(key, value);
        return options;
    }

    /** Mirrors what the table factory hands over: the DDL options plus the physical schema. */
    private static Map<String, String> ddlOptions() {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putTableSchema("schema", tableSchema());
        return new HashMap<>(descriptorProperties.asMap());
    }

    private static TableSchema tableSchema() {
        return TableSchema.builder()
                .field("f0", DataTypes.STRING())
                .field("f1", DataTypes.STRING())
                .field("f2", DataTypes.STRING())
                .field("f3", DataTypes.STRING())
                .field("f4", DataTypes.STRING())
                .field("f5", DataTypes.STRING())
                .build();
    }

    private static List<RowData> deserialize(Map<String, String> options, String body) {
        RowDeserializationSchema schema =
                new RowDeserializationSchema.Builder()
                        .setTableSchema(tableSchema())
                        .setProperties(options)
                        .build();
        schema.open(null);

        BytesMessage message = new BytesMessage();
        message.setData(body.getBytes(StandardCharsets.UTF_8));

        List<RowData> rows = new ArrayList<>();
        schema.deserialize(
                Collections.singletonList(message),
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        rows.add(record);
                    }

                    @Override
                    public void close() {}
                });
        return rows;
    }
}
