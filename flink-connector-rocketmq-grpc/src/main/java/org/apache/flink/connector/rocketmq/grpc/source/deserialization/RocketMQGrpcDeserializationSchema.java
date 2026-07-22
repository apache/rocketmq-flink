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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/** An interface for the deserialization of RocketMQ gRPC messages. */
@PublicEvolving
public interface RocketMQGrpcDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working method {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
        // Nothing to do here for the default implementation.
    }

    /**
     * Deserializes a {@link MessageView} and outputs zero or more records through the {@link
     * Collector}.
     *
     * @param messageView The MessageView to deserialize.
     * @param out The collector to put the resulting records.
     */
    void deserialize(MessageView messageView, Collector<T> out) throws IOException;

    /**
     * Create a {@link RocketMQGrpcDeserializationSchema} by wrapping a Flink {@link
     * DeserializationSchema}. The message body is deserialized using the given schema; the other
     * fields such as key, tag and properties are ignored.
     */
    static <T> RocketMQGrpcDeserializationSchema<T> flinkSchema(
            DeserializationSchema<T> deserializationSchema) {
        return new RocketMQGrpcDeserializationSchemaWrapper<>(deserializationSchema);
    }
}
