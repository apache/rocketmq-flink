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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the temporal columns of a RocketMQ table source are decoded. A {@code DATE}, {@code
 * TIME} or {@code TIMESTAMP} column is reported by the table schema as {@code java.time}, and a
 * value that the deserializer cannot convert is handled by the default dirty data strategy — the
 * row is dropped and the job keeps running.
 */
class RowDeserializationSchemaTemporalTypeTest {

    private static final String FIELD_DELIMITER_KEY = "rocketmq.client.message.field.delimiter";

    private static final String BODY = "42,43-name,2020-01-02,12:34:56,2020-01-02 03:04:05.123";

    @Test
    void dateColumnIsDecodedTest() {
        RowData row = deserializeRow(BODY);

        assertThat(row.getString(0).toString()).isEqualTo("42");
        assertThat(row.getInt(2)).isEqualTo((int) LocalDate.parse("2020-01-02").toEpochDay());
    }

    @Test
    void timeColumnIsDecodedTest() {
        RowData row = deserializeRow(BODY);

        // a TIME column is stored as the milliseconds of the day
        assertThat(row.getInt(3)).isEqualTo(12 * 3_600_000 + 34 * 60_000 + 56 * 1_000);
    }

    @Test
    void timestampColumnIsDecodedTest() {
        RowData row = deserializeRow(BODY);

        assertThat(row.getTimestamp(4, 3))
                .isEqualTo(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000)));
    }

    @Test
    void everyRowOfATemporalTableIsDeliveredTest() {
        List<RowData> rows =
                deserialize(BODY + "\n" + "43,44-name,2020-01-03,12:34:57,2020-01-03 03:04:06.000");

        assertThat(rows).hasSize(2);
    }

    @Test
    void unparsableDateIsStillSkippedTest() {
        // the fix must not turn a malformed value into a row of garbage: an unparsable field keeps
        // going through the configured dirty data strategy
        assertThat(deserialize("42,43-name,2020-13-45,12:34:56,2020-01-02 03:04:05.123")).isEmpty();
    }

    private static RowData deserializeRow(String body) {
        List<RowData> rows = deserialize(body);
        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    private static List<RowData> deserialize(String body) {
        Map<String, String> options = new HashMap<>();
        options.put(FIELD_DELIMITER_KEY, ",");
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(options);
        descriptorProperties.putTableSchema("schema", tableSchema());

        RowDeserializationSchema schema =
                new RowDeserializationSchema.Builder()
                        .setTableSchema(tableSchema())
                        .setProperties(new HashMap<>(descriptorProperties.asMap()))
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

    private static TableSchema tableSchema() {
        return TableSchema.builder()
                .field("id", DataTypes.STRING())
                .field("name", DataTypes.STRING())
                .field("dt", DataTypes.DATE())
                .field("tm", DataTypes.TIME())
                .field("ts", DataTypes.TIMESTAMP(3))
                .build();
    }
}
