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

package org.apache.flink.connector.rocketmq.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer serializer} for {@link RocketMQSourceSplit}. */
public class RocketMQPartitionSplitSerializer
        implements SimpleVersionedSerializer<RocketMQSourceSplit> {

    private static final int SNAPSHOT_VERSION = 0;
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQSourceSplit split) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            out.writeUTF(split.getTopic());
            out.writeUTF(split.getBrokerName());
            out.writeInt(split.getQueueId());
            out.writeLong(split.getStartingOffset());
            out.writeLong(split.getStoppingOffset());
            out.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public RocketMQSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(byteArrayInputStream)) {
            String topic = in.readUTF();
            String broker = in.readUTF();
            int partition = in.readInt();
            long startingOffset = in.readLong();
            long stoppingOffset = in.readLong();
            return new RocketMQSourceSplit(
                    topic, broker, partition, startingOffset, stoppingOffset);
        }
    }
}
