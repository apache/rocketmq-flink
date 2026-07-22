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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.grpc.sink.serialization.RocketMQGrpcSerializationSchema;
import org.apache.flink.connector.rocketmq.grpc.source.deserialization.RocketMQGrpcDeserializationSchema;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converts between RocketMQ gRPC {@link Message}/{@link MessageView} and Flink {@link RowData}. The
 * value part of the record is (de)serialized with the format-provided value schema; the message
 * body carries the encoded value. On the source side, requested metadata columns are appended after
 * the physical columns produced by the value format.
 */
@Internal
public class RocketMQGrpcRowDataConverter
        implements RocketMQGrpcDeserializationSchema<RowData>,
                RocketMQGrpcSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    @Nullable private final String topic;
    @Nullable private final String liteTopic;
    @Nullable private final DeserializationSchema<RowData> valueDeserialization;
    @Nullable private final SerializationSchema<RowData> valueSerialization;
    private final List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters;
    private final TypeInformation<RowData> producedType;

    private transient MetadataAppendingCollector metadataAppendingCollector;

    private RocketMQGrpcRowDataConverter(
            @Nullable String topic,
            @Nullable String liteTopic,
            @Nullable DeserializationSchema<RowData> valueDeserialization,
            @Nullable SerializationSchema<RowData> valueSerialization,
            List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters,
            @Nullable TypeInformation<RowData> producedType) {
        this.topic = topic;
        this.liteTopic = liteTopic;
        this.valueDeserialization = valueDeserialization;
        this.valueSerialization = valueSerialization;
        this.metadataConverters = metadataConverters;
        this.producedType = producedType;
    }

    /** Create a converter used by the table source to decode a {@link MessageView}. */
    public static RocketMQGrpcRowDataConverter forSource(
            DeserializationSchema<RowData> valueDeserialization,
            List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters,
            TypeInformation<RowData> producedType) {
        return new RocketMQGrpcRowDataConverter(
                null,
                null,
                checkNotNull(valueDeserialization),
                null,
                checkNotNull(metadataConverters),
                checkNotNull(producedType));
    }

    /** Create a converter used by the table sink to encode a {@link RowData}. */
    public static RocketMQGrpcRowDataConverter forSink(
            String topic, String liteTopic, SerializationSchema<RowData> valueSerialization) {
        return new RocketMQGrpcRowDataConverter(
                checkNotNull(topic),
                checkNotNull(liteTopic),
                null,
                checkNotNull(valueSerialization),
                Collections.emptyList(),
                null);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        checkNotNull(valueDeserialization).open(context);
        this.metadataAppendingCollector = new MetadataAppendingCollector(metadataConverters);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        checkNotNull(valueSerialization).open(context);
    }

    @Override
    public void deserialize(MessageView messageView, Collector<RowData> out) throws IOException {
        if (metadataConverters.isEmpty()) {
            checkNotNull(valueDeserialization).deserialize(messageView.getBody(), out);
            return;
        }
        metadataAppendingCollector.reset(messageView, out);
        checkNotNull(valueDeserialization)
                .deserialize(messageView.getBody(), metadataAppendingCollector);
    }

    @Override
    public Message serialize(RowData element, MessageBuilder messageBuilder, Long timestamp) {
        final byte[] body = checkNotNull(valueSerialization).serialize(element);
        return messageBuilder.setTopic(topic).setLiteTopic(liteTopic).setBody(body).build();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return checkNotNull(producedType);
    }

    /**
     * Appends the requested metadata columns after the physical columns of each row produced by the
     * value format. Like other connectors' metadata support, it relies on the format's runtime
     * decoder producing {@link GenericRowData}.
     */
    private static final class MetadataAppendingCollector implements Collector<RowData> {

        private final List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters;

        private MessageView messageView;
        private Collector<RowData> out;

        private MetadataAppendingCollector(
                List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters) {
            this.metadataConverters = metadataConverters;
        }

        private void reset(MessageView messageView, Collector<RowData> out) {
            this.messageView = messageView;
            this.out = out;
        }

        @Override
        public void collect(RowData physicalRow) {
            final GenericRowData physical = (GenericRowData) physicalRow;
            final int physicalArity = physical.getArity();
            final GenericRowData produced =
                    new GenericRowData(
                            physical.getRowKind(), physicalArity + metadataConverters.size());
            for (int pos = 0; pos < physicalArity; pos++) {
                produced.setField(pos, physical.getField(pos));
            }
            for (int pos = 0; pos < metadataConverters.size(); pos++) {
                produced.setField(
                        physicalArity + pos, metadataConverters.get(pos).read(messageView));
            }
            out.collect(produced);
        }

        @Override
        public void close() {}
    }
}
