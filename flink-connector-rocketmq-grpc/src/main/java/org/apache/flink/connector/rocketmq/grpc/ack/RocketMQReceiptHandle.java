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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * A self-contained, serializable descriptor of a RocketMQ Pop message that carries everything a
 * downstream operator needs to acknowledge (or extend the invisible duration of) the message
 * without holding on to the SDK message object or any credentials.
 *
 * <p>The RocketMQ 5.x Pop model lets any consumer in the same group acknowledge a message given its
 * receipt handle and routing information; the message does not have to be acked by the consumer
 * that originally received it. This class captures exactly that routing information so that the
 * acknowledgement decision can be moved to any downstream operator:
 *
 * <ul>
 *   <li>{@code endpoint}, {@code namespace}, {@code consumerGroup} — the routing triple that
 *       selects which pooled consumer must issue the ack RPC. Multiple sources (possibly pointing
 *       at different clusters/namespaces/groups) can coexist because each message carries its own
 *       triple.
 *   <li>{@code topic}, {@code liteTopic}, {@code messageId}, {@code receiptHandle}, {@code
 *       deliveryAttempt} — the per-message fields required to rebuild the SDK message view used by
 *       the ack/changeInvisibleDuration calls.
 * </ul>
 *
 * <p>This type intentionally contains <b>no</b> credentials, no protobuf blob and no message body,
 * so it is safe to ship across the Flink data stream.
 */
@PublicEvolving
public final class RocketMQReceiptHandle implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String namespace;
    private final String consumerGroup;
    private final String topic;
    /** The lite (sub) topic, or {@code null} when the message was not received via a lite topic. */
    private final String liteTopic;

    private final String messageId;
    private final String receiptHandle;
    private final int deliveryAttempt;

    public RocketMQReceiptHandle(
            String endpoint,
            String namespace,
            String consumerGroup,
            String topic,
            String liteTopic,
            String messageId,
            String receiptHandle,
            int deliveryAttempt) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint should not be null");
        this.namespace = Objects.requireNonNull(namespace, "namespace should not be null");
        this.consumerGroup =
                Objects.requireNonNull(consumerGroup, "consumerGroup should not be null");
        this.topic = Objects.requireNonNull(topic, "topic should not be null");
        this.liteTopic = liteTopic;
        this.messageId = Objects.requireNonNull(messageId, "messageId should not be null");
        this.receiptHandle =
                Objects.requireNonNull(receiptHandle, "receiptHandle should not be null");
        this.deliveryAttempt = deliveryAttempt;
    }

    /**
     * The proxy endpoint(s) the message came from, as a {@code host:port[;host:port...]} string
     * that can be parsed back into the SDK endpoints.
     */
    public String getEndpoint() {
        return endpoint;
    }

    /** The resource namespace of the RocketMQ instance ({@code ""} when none is configured). */
    public String getNamespace() {
        return namespace;
    }

    /**
     * The consumer group the source used; the ack must be issued by a consumer of the same group.
     */
    public String getConsumerGroup() {
        return consumerGroup;
    }

    /** The (physical) topic the message belongs to. */
    public String getTopic() {
        return topic;
    }

    /** The lite (sub) topic, or {@code null} when the message was not received via a lite topic. */
    public String getLiteTopic() {
        return liteTopic;
    }

    /** The unique message id. */
    public String getMessageId() {
        return messageId;
    }

    /** The Pop receipt handle used to acknowledge or re-schedule the message. */
    public String getReceiptHandle() {
        return receiptHandle;
    }

    /** The number of times the message has been delivered. */
    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMQReceiptHandle that = (RocketMQReceiptHandle) o;
        return deliveryAttempt == that.deliveryAttempt
                && endpoint.equals(that.endpoint)
                && namespace.equals(that.namespace)
                && consumerGroup.equals(that.consumerGroup)
                && topic.equals(that.topic)
                && Objects.equals(liteTopic, that.liteTopic)
                && messageId.equals(that.messageId)
                && receiptHandle.equals(that.receiptHandle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                endpoint,
                namespace,
                consumerGroup,
                topic,
                liteTopic,
                messageId,
                receiptHandle,
                deliveryAttempt);
    }

    @Override
    public String toString() {
        return "RocketMQReceiptHandle{"
                + "endpoint='"
                + endpoint
                + '\''
                + ", namespace='"
                + namespace
                + '\''
                + ", consumerGroup='"
                + consumerGroup
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", liteTopic='"
                + liteTopic
                + '\''
                + ", messageId='"
                + messageId
                + '\''
                + ", deliveryAttempt="
                + deliveryAttempt
                + '}';
    }

    /**
     * A stateless {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link
     * RocketMQReceiptHandle}. It writes the eight self-contained fields directly; the nullable
     * {@code liteTopic} is guarded by a boolean flag.
     */
    @Internal
    public static final class Serializer extends TypeSerializerSingleton<RocketMQReceiptHandle> {

        private static final long serialVersionUID = 1L;

        public static final Serializer INSTANCE = new Serializer();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public RocketMQReceiptHandle createInstance() {
            return null;
        }

        @Override
        public RocketMQReceiptHandle copy(RocketMQReceiptHandle from) {
            // RocketMQReceiptHandle is immutable.
            return from;
        }

        @Override
        public RocketMQReceiptHandle copy(RocketMQReceiptHandle from, RocketMQReceiptHandle reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(RocketMQReceiptHandle record, DataOutputView target)
                throws IOException {
            target.writeUTF(record.getEndpoint());
            target.writeUTF(record.getNamespace());
            target.writeUTF(record.getConsumerGroup());
            target.writeUTF(record.getTopic());
            final String liteTopic = record.getLiteTopic();
            if (liteTopic == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeUTF(liteTopic);
            }
            target.writeUTF(record.getMessageId());
            target.writeUTF(record.getReceiptHandle());
            target.writeInt(record.getDeliveryAttempt());
        }

        @Override
        public RocketMQReceiptHandle deserialize(DataInputView source) throws IOException {
            final String endpoint = source.readUTF();
            final String namespace = source.readUTF();
            final String consumerGroup = source.readUTF();
            final String topic = source.readUTF();
            final String liteTopic = source.readBoolean() ? source.readUTF() : null;
            final String messageId = source.readUTF();
            final String receiptHandle = source.readUTF();
            final int deliveryAttempt = source.readInt();
            return new RocketMQReceiptHandle(
                    endpoint,
                    namespace,
                    consumerGroup,
                    topic,
                    liteTopic,
                    messageId,
                    receiptHandle,
                    deliveryAttempt);
        }

        @Override
        public RocketMQReceiptHandle deserialize(RocketMQReceiptHandle reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            serialize(deserialize(source), target);
        }

        @Override
        public TypeSerializerSnapshot<RocketMQReceiptHandle> snapshotConfiguration() {
            return new SerializerSnapshot();
        }

        /** Serializer snapshot for {@link Serializer}. */
        public static final class SerializerSnapshot
                extends SimpleTypeSerializerSnapshot<RocketMQReceiptHandle> {

            public SerializerSnapshot() {
                super(() -> INSTANCE);
            }
        }
    }
}
