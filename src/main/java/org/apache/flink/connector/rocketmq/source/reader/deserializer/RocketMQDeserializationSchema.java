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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/** An interface for the deserialization of RocketMQ records. */
@PublicEvolving
public interface RocketMQDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided {@link InitializationContext} can be used to access additional features such
     * as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
        // Nothing to do here for the default implementation.
    }

    /**
     * Deserializes the byte message.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param messageView The MessageView to deserialize.
     * @param out The collector to put the resulting messages.
     */
    void deserialize(MessageView messageView, Collector<T> out) throws IOException;

    /**
     * Create a RocketMQDeserializationSchema by using the flink's {@link DeserializationSchema}. It
     * would consume the rocketmq message as byte array and decode the message by using flink's
     * logic.
     */
    static <T> RocketMQDeserializationSchema<T> flinkSchema(
            DeserializationSchema<T> deserializationSchema) {
        return new RocketMQDeserializationSchemaWrapper<>(deserializationSchema);
    }

    /**
     * Wraps a {@link DeserializationSchema} as the value deserialization schema. The other fields
     * such as key, headers, timestamp are ignored.
     *
     * @param deserializationSchema the {@link DeserializationSchema} used to deserialize the value
     *     of a {@link RocketMQDeserializationSchemaWrapper}.
     * @param <T> the type of the deserialized record.
     */
    static <T> RocketMQDeserializationSchema<T> flinkBodyOnlySchema(
            DeserializationSchema<T> deserializationSchema) {
        return new RocketMQDeserializationSchemaWrapper<>(deserializationSchema);
    }

    static <T> RocketMQDeserializationSchema<T> rocketMQSchema(
            DeserializationSchema<T> valueDeserializationSchema) {
        return new RocketMQDeserializationSchemaWrapper<>(valueDeserializationSchema);
    }
}
