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

package org.apache.flink.connector.rocketmq.source.enumerator;

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of RocketMQ source. */
public class RocketMQSourceEnumStateSerializer
        implements SimpleVersionedSerializer<RocketMQSourceEnumState> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocketMQSourceEnumStateSerializer.class);

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQSourceEnumState enumState) throws IOException {
        Set<MessageQueue> assignments = enumState.getCurrentSplitAssignment();

        LOG.info("Serializer enumerator state, {}", JSON.toJSONString(enumState));

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(assignments.size());
            for (MessageQueue assignment : assignments) {
                out.writeUTF(assignment.getBrokerName());
                out.writeUTF(assignment.getTopic());
                out.writeInt(assignment.getQueueId());
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public RocketMQSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        // Check whether the version of serialized bytes is supported.
        if (version == CURRENT_VERSION) {
            Set<MessageQueue> queues = deserializeMessageQueue(serialized);
            return new RocketMQSourceEnumState(queues);
        }
        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, getVersion()));
    }

    private Set<MessageQueue> deserializeMessageQueue(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            int size = in.readInt();
            HashSet<MessageQueue> result = new HashSet<>();
            for (int i = 0; i < size; i++) {
                String brokerName = in.readUTF();
                String topic = in.readUTF();
                int queueId = in.readInt();

                MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                result.add(queue);
            }

            return result;
        }
    }
}
