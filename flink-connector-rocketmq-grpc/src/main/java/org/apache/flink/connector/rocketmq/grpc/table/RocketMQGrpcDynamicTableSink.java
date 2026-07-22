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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.sink.RocketMQGrpcSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Objects;

/** A {@link DynamicTableSink} for the RocketMQ gRPC connector (at-least-once). */
@Internal
public class RocketMQGrpcDynamicTableSink implements DynamicTableSink {

    private final Configuration configuration;
    private final String topic;
    private final String liteTopic;
    private final DataType physicalDataType;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    public RocketMQGrpcDynamicTableSink(
            Configuration configuration,
            String topic,
            String liteTopic,
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
        this.configuration = configuration;
        this.topic = topic;
        this.liteTopic = liteTopic;
        this.physicalDataType = physicalDataType;
        this.encodingFormat = encodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> valueSerialization =
                encodingFormat.createRuntimeEncoder(context, physicalDataType);
        final RocketMQGrpcRowDataConverter converter =
                RocketMQGrpcRowDataConverter.forSink(topic, liteTopic, valueSerialization);

        final RocketMQGrpcSink<RowData> sink =
                RocketMQGrpcSink.<RowData>builder()
                        .setConfig(configuration)
                        .setSerializer(converter)
                        .build();

        return SinkV2Provider.of(sink);
    }

    @Override
    public DynamicTableSink copy() {
        return new RocketMQGrpcDynamicTableSink(
                configuration, topic, liteTopic, physicalDataType, encodingFormat);
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
        final RocketMQGrpcDynamicTableSink that = (RocketMQGrpcDynamicTableSink) o;
        return Objects.equals(configuration, that.configuration)
                && Objects.equals(topic, that.topic)
                && Objects.equals(liteTopic, that.liteTopic)
                && Objects.equals(physicalDataType, that.physicalDataType)
                && Objects.equals(encodingFormat, that.encodingFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, topic, liteTopic, physicalDataType, encodingFormat);
    }
}
