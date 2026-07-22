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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocketMQReceiptHandle.Serializer}. */
class RocketMQReceiptHandleSerializerTest {

    private final RocketMQReceiptHandle.Serializer serializer =
            RocketMQReceiptHandle.Serializer.INSTANCE;

    @Test
    void roundTripWithLiteTopic() throws IOException {
        final RocketMQReceiptHandle handle =
                new RocketMQReceiptHandle(
                        "127.0.0.1:8080", "ns", "grp", "topic", "sub", "mid", "rh", 3);

        assertThat(roundTrip(handle)).isEqualTo(handle);
    }

    @Test
    void roundTripWithNullLiteTopic() throws IOException {
        final RocketMQReceiptHandle handle =
                new RocketMQReceiptHandle(
                        "127.0.0.1:8080", "", "grp", "topic", null, "mid", "rh", 0);

        final RocketMQReceiptHandle deserialized = roundTrip(handle);
        assertThat(deserialized.getLiteTopic()).isNull();
        assertThat(deserialized).isEqualTo(handle);
    }

    @Test
    void snapshotIsCompatibleAsIs() {
        final TypeSerializerSnapshot<RocketMQReceiptHandle> snapshot =
                serializer.snapshotConfiguration();
        final TypeSerializerSchemaCompatibility<RocketMQReceiptHandle> compatibility =
                serializer.snapshotConfiguration().resolveSchemaCompatibility(snapshot);
        assertThat(compatibility.isCompatibleAsIs()).isTrue();
    }

    private RocketMQReceiptHandle roundTrip(RocketMQReceiptHandle handle) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(handle, out);
        final DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return serializer.deserialize(in);
    }
}
