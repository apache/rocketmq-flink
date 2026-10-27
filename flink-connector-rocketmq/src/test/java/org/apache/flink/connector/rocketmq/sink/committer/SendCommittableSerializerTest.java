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

package org.apache.flink.connector.rocketmq.sink.committer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SendCommittableSerializer}. The address of the node that accepted the half
 * message travels with the committable because it survives a checkpoint; losing it on the way would
 * silently send every commit to whatever node the committer's route table happens to name.
 */
class SendCommittableSerializerTest {

    private final SendCommittableSerializer serializer = new SendCommittableSerializer();

    @Test
    void brokerAddressSurvivesRoundTripTest() throws Exception {
        SendCommittable committable = sendCommittable();
        committable.setBrokerAddress("10.0.38.29:8080");

        SendCommittable restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(restored.getBrokerAddress()).isEqualTo("10.0.38.29:8080");
        assertThat(restored.getTopic()).isEqualTo(committable.getTopic());
        assertThat(restored.getBrokerName()).isEqualTo(committable.getBrokerName());
        assertThat(restored.getQueueId()).isEqualTo(committable.getQueueId());
        assertThat(restored.getQueueOffset()).isEqualTo(committable.getQueueOffset());
        assertThat(restored.getMsgId()).isEqualTo(committable.getMsgId());
        assertThat(restored.getOffsetMsgId()).isEqualTo(committable.getOffsetMsgId());
        assertThat(restored.getTransactionId()).isEqualTo(committable.getTransactionId());
    }

    @Test
    void versionOnePayloadIsStillReadableTest() throws Exception {
        SendCommittable restored = serializer.deserialize(1, versionOneBytes());

        assertThat(restored.getTransactionId()).isEqualTo("trans-1");
        assertThat(restored.getBrokerAddress()).isNull();
    }

    /** The version 1 layout: the same fields, without the trailing address. */
    private static byte[] versionOneBytes() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(stream)) {
            out.writeUTF("TopicTest");
            out.writeUTF("broker-a");
            out.writeInt(3);
            out.writeLong(42L);
            out.writeUTF("msg-1");
            out.writeUTF("offset-msg-1");
            out.writeUTF("trans-1");
            out.flush();
        }
        return stream.toByteArray();
    }

    private static SendCommittable sendCommittable() {
        SendCommittable committable = new SendCommittable();
        committable.setTopic("TopicTest");
        committable.setBrokerName("broker-a");
        committable.setQueueId(3);
        committable.setQueueOffset(42L);
        committable.setMsgId("msg-1");
        committable.setOffsetMsgId("offset-msg-1");
        committable.setTransactionId("trans-1");
        return committable;
    }
}
