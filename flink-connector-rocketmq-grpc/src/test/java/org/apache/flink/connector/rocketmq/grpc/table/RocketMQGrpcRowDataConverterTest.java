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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the metadata handling of {@link RocketMQGrpcRowDataConverter}. */
class RocketMQGrpcRowDataConverterTest {

    @Test
    void appendsRequestedMetadataColumnsTest() throws Exception {
        final List<RocketMQGrpcReadableMetadata.MetadataConverter> converters =
                Arrays.asList(
                        RocketMQGrpcReadableMetadata.TAG.getConverter(),
                        RocketMQGrpcReadableMetadata.DELIVERY_ATTEMPT.getConverter(),
                        RocketMQGrpcReadableMetadata.BORN_TIMESTAMP.getConverter());
        final RocketMQGrpcRowDataConverter converter =
                RocketMQGrpcRowDataConverter.forSource(
                        new BodyAsStringDeserializationSchema(),
                        converters,
                        TypeInformation.of(RowData.class));
        converter.open((DeserializationSchema.InitializationContext) null);

        final List<RowData> output = new ArrayList<>();
        converter.deserialize(new TestingMessageView(), new ListCollector(output));

        assertThat(output).hasSize(1);
        final GenericRowData row = (GenericRowData) output.get(0);
        assertThat(row.getArity()).isEqualTo(4);
        assertThat(row.getField(0)).isEqualTo(StringData.fromString("body"));
        assertThat(row.getField(1)).isEqualTo(StringData.fromString("tagA"));
        assertThat(row.getField(2)).isEqualTo(7);
        assertThat(row.getField(3)).isEqualTo(TimestampData.fromEpochMillis(1234L));
    }

    @Test
    void forwardsPhysicalRowWithoutMetadataTest() throws Exception {
        final RocketMQGrpcRowDataConverter converter =
                RocketMQGrpcRowDataConverter.forSource(
                        new BodyAsStringDeserializationSchema(),
                        Collections.emptyList(),
                        TypeInformation.of(RowData.class));
        converter.open((DeserializationSchema.InitializationContext) null);

        final List<RowData> output = new ArrayList<>();
        converter.deserialize(new TestingMessageView(), new ListCollector(output));

        assertThat(output).hasSize(1);
        final GenericRowData row = (GenericRowData) output.get(0);
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(StringData.fromString("body"));
    }

    /** Decodes the message body into a single-field row containing the body as a string. */
    private static class BodyAsStringDeserializationSchema
            implements DeserializationSchema<RowData> {

        private static final long serialVersionUID = 1L;

        @Override
        public RowData deserialize(byte[] message) {
            return GenericRowData.of(
                    StringData.fromString(new String(message, StandardCharsets.UTF_8)));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    private static class ListCollector implements Collector<RowData> {

        private final List<RowData> output;

        private ListCollector(List<RowData> output) {
            this.output = output;
        }

        @Override
        public void collect(RowData record) {
            output.add(record);
        }

        @Override
        public void close() {}
    }

    /** A connector-level message view with fixed attributes. */
    private static class TestingMessageView implements MessageView {

        @Override
        public String getMessageId() {
            return "MSG-1";
        }

        @Override
        public String getTopic() {
            return "topic";
        }

        @Override
        public String getTag() {
            return "tagA";
        }

        @Override
        public Collection<String> getKeys() {
            return Arrays.asList("k1", "k2");
        }

        @Override
        public byte[] getBody() {
            return "body".getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public int getDeliveryAttempt() {
            return 7;
        }

        @Override
        public long getEventTime() {
            return 1234L;
        }

        @Override
        public Map<String, String> getProperties() {
            return Collections.singletonMap("p1", "v1");
        }
    }
}
