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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessage;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A {@link ScanTableSource} for the RocketMQ gRPC connector. */
@Internal
public class RocketMQGrpcDynamicTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final Configuration configuration;
    private final String topic;
    private final Boundedness boundedness;
    private final DataType physicalDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    /** The metadata keys applied by {@link #applyReadableMetadata(List, DataType)}. */
    private List<String> metadataKeys;

    /** The produced data type including the appended metadata columns. */
    private DataType producedDataType;

    public RocketMQGrpcDynamicTableSource(
            Configuration configuration,
            String topic,
            Boundedness boundedness,
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.configuration = configuration;
        this.topic = topic;
        this.boundedness = boundedness;
        this.physicalDataType = physicalDataType;
        this.decodingFormat = decodingFormat;
        this.metadataKeys = Collections.emptyList();
        this.producedDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(RocketMQGrpcReadableMetadata.values())
                .forEach(metadata -> metadataMap.put(metadata.getKey(), metadata.getDataType()));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> valueDeserialization =
                decodingFormat.createRuntimeDecoder(context, physicalDataType);
        final TypeInformation<RowData> producedType =
                context.createTypeInformation(producedDataType);
        final List<RocketMQGrpcReadableMetadata.MetadataConverter> metadataConverters =
                metadataKeys.stream()
                        .map(
                                key ->
                                        Stream.of(RocketMQGrpcReadableMetadata.values())
                                                .filter(metadata -> metadata.getKey().equals(key))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new)
                                                .getConverter())
                        .collect(Collectors.toList());
        final RocketMQGrpcRowDataConverter converter =
                RocketMQGrpcRowDataConverter.forSource(
                        valueDeserialization, metadataConverters, producedType);

        final RocketMQGrpcSource<RowData> source =
                RocketMQGrpcSource.<RowData>builder()
                        .setConfig(configuration)
                        .setMainTopic(topic)
                        .setBoundedness(boundedness)
                        .setDeserializer(converter)
                        .build();

        // The source produces AckableMessage<RowData>; the SQL/Table path does not perform
        // downstream acknowledgement, so the receipt handle is stripped here and only the RowData
        // value is forwarded.
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                final DataStreamSource<AckableMessage<RowData>> sourceStream =
                        execEnv.fromSource(
                                source, WatermarkStrategy.noWatermarks(), "RocketMQGrpcSource");
                final SingleOutputStreamOperator<RowData> valueStream =
                        sourceStream.map(AckableMessage::getValue).returns(producedType);
                providerContext.generateUid("rocketmq-grpc-source").ifPresent(valueStream::uid);
                return valueStream;
            }

            @Override
            public boolean isBounded() {
                return Boundedness.BOUNDED == boundedness;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        final RocketMQGrpcDynamicTableSource copy =
                new RocketMQGrpcDynamicTableSource(
                        configuration, topic, boundedness, physicalDataType, decodingFormat);
        copy.metadataKeys = new ArrayList<>(metadataKeys);
        copy.producedDataType = producedDataType;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "RocketMQGrpc";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RocketMQGrpcDynamicTableSource that = (RocketMQGrpcDynamicTableSource) o;
        return Objects.equals(configuration, that.configuration)
                && Objects.equals(topic, that.topic)
                && boundedness == that.boundedness
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(decodingFormat, that.decodingFormat)
                && Objects.equals(metadataKeys, that.metadataKeys)
                && Objects.equals(producedDataType, that.producedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                configuration,
                topic,
                boundedness,
                physicalDataType,
                decodingFormat,
                metadataKeys,
                producedDataType);
    }
}
