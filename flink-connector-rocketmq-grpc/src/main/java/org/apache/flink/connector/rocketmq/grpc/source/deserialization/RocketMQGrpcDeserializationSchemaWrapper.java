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

package org.apache.flink.connector.rocketmq.grpc.source.deserialization;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link RocketMQGrpcDeserializationSchema} that adapts a Flink {@link DeserializationSchema} by
 * deserializing the message body only.
 */
@Internal
public class RocketMQGrpcDeserializationSchemaWrapper<T>
        implements RocketMQGrpcDeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<T> deserializationSchema;

    public RocketMQGrpcDeserializationSchemaWrapper(
            DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = checkNotNull(deserializationSchema);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        deserializationSchema.open(context);
    }

    @Override
    public void deserialize(MessageView messageView, Collector<T> out) throws IOException {
        deserializationSchema.deserialize(messageView.getBody(), out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
