/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rocketmq.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.table.data.RowData;

import org.apache.rocketmq.common.message.Message;

/**
 * A {@link RocketMQSerializationSchema} adapter that bridges the Table/SQL {@link RowData} to
 * RocketMQ {@link Message} conversion. Delegates to {@link RocketMQRowDataConverter} for the actual
 * conversion logic.
 */
public class RocketMQRowDataSerializationSchema implements RocketMQSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RocketMQRowDataConverter converter;

    public RocketMQRowDataSerializationSchema(RocketMQRowDataConverter converter) {
        this.converter = converter;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, RocketMQSinkContext sinkContext)
            throws Exception {
        converter.open();
    }

    @Override
    public Message serialize(RowData element, RocketMQSinkContext context, Long timestamp) {
        return converter.convert(element);
    }
}
