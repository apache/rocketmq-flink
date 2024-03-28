/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** A serializer used to serialize {@link SendCommittable}. */
public class SendCommittableSerializer implements SimpleVersionedSerializer<SendCommittable> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SendCommittable obj) throws IOException {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(stream)) {
            out.writeUTF(obj.getTopic());
            out.writeUTF(obj.getBrokerName());
            out.writeInt(obj.getQueueId());
            out.writeLong(obj.getQueueOffset());
            out.writeUTF(obj.getMsgId());
            out.writeUTF(obj.getOffsetMsgId());
            out.writeUTF(obj.getTransactionId());
            out.flush();
            return stream.toByteArray();
        }
    }

    @Override
    public SendCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bis)) {
            SendCommittable sendCommittable = new SendCommittable();
            sendCommittable.setTopic(in.readUTF());
            sendCommittable.setBrokerName(in.readUTF());
            sendCommittable.setQueueId(in.readInt());
            sendCommittable.setQueueOffset(in.readLong());
            sendCommittable.setMsgId(in.readUTF());
            sendCommittable.setOffsetMsgId(in.readUTF());
            sendCommittable.setTransactionId(in.readUTF());
            return sendCommittable;
        }
    }
}
