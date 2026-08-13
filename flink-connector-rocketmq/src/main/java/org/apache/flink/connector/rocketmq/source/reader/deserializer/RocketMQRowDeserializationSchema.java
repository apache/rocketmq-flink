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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RowDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A row data wrapper class that wraps a {@link RocketMQDeserializationSchema} to deserialize {@link
 * MessageView}.
 */
public class RocketMQRowDeserializationSchema implements RocketMQDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowDeserializationSchema deserializationSchema;

    private transient List<BytesMessage> bytesMessages = new ArrayList<>(1);

    public RocketMQRowDeserializationSchema(
            TableSchema tableSchema,
            Map<String, String> properties,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters) {
        deserializationSchema =
                new RowDeserializationSchema.Builder()
                        .setProperties(properties)
                        .setTableSchema(tableSchema)
                        .setHasMetadata(hasMetadata)
                        .setMetadataConverters(metadataConverters)
                        .build();
    }

    @Override
    public void open(InitializationContext context) {
        deserializationSchema.open(context);
        bytesMessages = new ArrayList<>();
    }

    @Override
    public void deserialize(MessageView messageView, Collector<RowData> collector)
            throws IOException {
        BytesMessage bytesMessage = toBytesMessage(messageView);
        bytesMessages = new ArrayList<>(1);
        bytesMessages.add(bytesMessage);
        deserializationSchema.deserialize(bytesMessages, collector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    private static BytesMessage toBytesMessage(MessageView message) {
        BytesMessage bytesMessage = new BytesMessage();
        bytesMessage.setData(message.getBody());
        if (message.getProperties() != null) {
            bytesMessage.setProperties(message.getProperties());
        }
        bytesMessage.setProperty("__topic__", message.getTopic());
        bytesMessage.setProperty("__store_timestamp__", String.valueOf(message.getIngestionTime()));
        bytesMessage.setProperty("__born_timestamp__", String.valueOf(message.getEventTime()));
        bytesMessage.setProperty("__queue_id__", String.valueOf(message.getQueueId()));
        bytesMessage.setProperty("__queue_offset__", String.valueOf(message.getQueueOffset()));
        bytesMessage.setProperty("__msg_id__", message.getMessageId());
        if (message.getKeys() != null) {
            bytesMessage.setProperty("__keys__", String.join(" ", message.getKeys()));
        }
        bytesMessage.setProperty("__tags__", message.getTag());
        return bytesMessage;
    }

    @VisibleForTesting
    public List<BytesMessage> getBytesMessages() {
        return bytesMessages;
    }
}
