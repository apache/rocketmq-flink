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

package org.apache.flink.connector.rocketmq.grpc.ack;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AckableMessageTypeInfo.Serializer}. */
class AckableMessageSerializerTest {

    private final AckableMessageTypeInfo.Serializer<String> serializer =
            new AckableMessageTypeInfo.Serializer<>(StringSerializer.INSTANCE);

    private static RocketMQReceiptHandle handle() {
        return new RocketMQReceiptHandle(
                "127.0.0.1:8080", "ns", "grp", "topic", "sub", "mid", "rh", 1);
    }

    @Test
    void roundTripWithValue() throws IOException {
        final AckableMessage<String> message = new AckableMessage<>("payload", handle());
        assertThat(roundTrip(message)).isEqualTo(message);
    }

    @Test
    void roundTripWithNullValue() throws IOException {
        final AckableMessage<String> message = new AckableMessage<>(null, handle());
        final AckableMessage<String> deserialized = roundTrip(message);
        assertThat(deserialized.getValue()).isNull();
        assertThat(deserialized).isEqualTo(message);
    }

    @Test
    void snapshotIsCompatibleWithSameValueSerializer() {
        final TypeSerializerSnapshot<AckableMessage<String>> snapshot =
                serializer.snapshotConfiguration();
        final AckableMessageTypeInfo.Serializer<String> other =
                new AckableMessageTypeInfo.Serializer<>(StringSerializer.INSTANCE);
        final TypeSerializerSchemaCompatibility<AckableMessage<String>> compatibility =
                other.snapshotConfiguration().resolveSchemaCompatibility(snapshot);
        assertThat(compatibility.isCompatibleAsIs()).isTrue();
    }

    private AckableMessage<String> roundTrip(AckableMessage<String> message) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(message, out);
        final DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return serializer.deserialize(in);
    }
}
