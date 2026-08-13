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
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The readable metadata of a RocketMQ gRPC message that a SQL table source can expose as {@code
 * METADATA VIRTUAL} columns.
 */
@Internal
public enum RocketMQGrpcReadableMetadata {
    MESSAGE_ID(
            "message_id",
            DataTypes.STRING().nullable(),
            messageView -> StringData.fromString(messageView.getMessageId())),

    TOPIC(
            "topic",
            DataTypes.STRING().nullable(),
            messageView -> StringData.fromString(messageView.getTopic())),

    TAG(
            "tag",
            DataTypes.STRING().nullable(),
            messageView ->
                    messageView.getTag() == null
                            ? null
                            : StringData.fromString(messageView.getTag())),

    KEYS(
            "keys",
            DataTypes.ARRAY(DataTypes.STRING()).nullable(),
            messageView ->
                    new GenericArrayData(
                            messageView.getKeys().stream().map(StringData::fromString).toArray())),

    DELIVERY_ATTEMPT(
            "delivery_attempt", DataTypes.INT().nullable(), MessageView::getDeliveryAttempt),

    BORN_TIMESTAMP(
            "born_timestamp",
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
            messageView -> TimestampData.fromEpochMillis(messageView.getEventTime())),

    PROPERTIES(
            "properties",
            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()).nullable(),
            messageView -> {
                final Map<StringData, StringData> map = new HashMap<>();
                messageView
                        .getProperties()
                        .forEach(
                                (key, value) ->
                                        map.put(
                                                StringData.fromString(key),
                                                StringData.fromString(value)));
                return new GenericMapData(map);
            });

    private final String key;
    private final DataType dataType;
    private final MetadataConverter converter;

    RocketMQGrpcReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public MetadataConverter getConverter() {
        return converter;
    }

    /** Converts a message attribute into the internal data structure of the metadata column. */
    @FunctionalInterface
    public interface MetadataConverter extends Serializable {
        @Nullable
        Object read(MessageView messageView);
    }
}
