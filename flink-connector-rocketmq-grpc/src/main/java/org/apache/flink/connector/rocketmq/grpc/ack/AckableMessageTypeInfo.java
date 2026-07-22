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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * The {@link TypeInformation} for {@link AckableMessage}. It carries the value {@link
 * TypeInformation} so that Flink can derive a serializer for {@code T} while the receipt handle is
 * serialized by the dedicated {@link RocketMQReceiptHandle.Serializer}.
 *
 * @param <T> the deserialized record type.
 */
@PublicEvolving
public final class AckableMessageTypeInfo<T> extends TypeInformation<AckableMessage<T>> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<T> valueTypeInfo;

    public AckableMessageTypeInfo(TypeInformation<T> valueTypeInfo) {
        this.valueTypeInfo = Objects.requireNonNull(valueTypeInfo, "valueTypeInfo");
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getTotalFields() {
        return valueTypeInfo.getTotalFields() + 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<AckableMessage<T>> getTypeClass() {
        return (Class<AckableMessage<T>>) (Class<?>) AckableMessage.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<AckableMessage<T>> createSerializer(ExecutionConfig config) {
        return new Serializer<>(valueTypeInfo.createSerializer(config));
    }

    @Override
    public String toString() {
        return "AckableMessageTypeInfo<" + valueTypeInfo + '>';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AckableMessageTypeInfo<?> that = (AckableMessageTypeInfo<?>) obj;
        return valueTypeInfo.equals(that.valueTypeInfo);
    }

    @Override
    public int hashCode() {
        return valueTypeInfo.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof AckableMessageTypeInfo;
    }

    /**
     * A {@link TypeSerializer} for {@link AckableMessage} that composes a caller-supplied value
     * serializer with the {@link RocketMQReceiptHandle.Serializer}. A {@code null} value is
     * supported via a boolean flag so that the value serializer never has to encode {@code null}.
     *
     * @param <T> the deserialized record type.
     */
    @Internal
    public static final class Serializer<T> extends TypeSerializer<AckableMessage<T>> {

        private static final long serialVersionUID = 1L;

        private final TypeSerializer<T> valueSerializer;
        private final TypeSerializer<RocketMQReceiptHandle> handleSerializer;

        public Serializer(TypeSerializer<T> valueSerializer) {
            this(valueSerializer, RocketMQReceiptHandle.Serializer.INSTANCE);
        }

        Serializer(
                TypeSerializer<T> valueSerializer,
                TypeSerializer<RocketMQReceiptHandle> handleSerializer) {
            this.valueSerializer = valueSerializer;
            this.handleSerializer = handleSerializer;
        }

        @Override
        public boolean isImmutableType() {
            return valueSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<AckableMessage<T>> duplicate() {
            final TypeSerializer<T> duplicatedValue = valueSerializer.duplicate();
            if (duplicatedValue == valueSerializer) {
                return this;
            }
            return new Serializer<>(duplicatedValue, handleSerializer.duplicate());
        }

        @Override
        public AckableMessage<T> createInstance() {
            return null;
        }

        @Override
        public AckableMessage<T> copy(AckableMessage<T> from) {
            final T value = from.getValue();
            final T copiedValue = value == null ? null : valueSerializer.copy(value);
            return new AckableMessage<>(copiedValue, from.getHandle());
        }

        @Override
        public AckableMessage<T> copy(AckableMessage<T> from, AckableMessage<T> reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(AckableMessage<T> record, DataOutputView target) throws IOException {
            final T value = record.getValue();
            if (value == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                valueSerializer.serialize(value, target);
            }
            handleSerializer.serialize(record.getHandle(), target);
        }

        @Override
        public AckableMessage<T> deserialize(DataInputView source) throws IOException {
            final T value = source.readBoolean() ? valueSerializer.deserialize(source) : null;
            final RocketMQReceiptHandle handle = handleSerializer.deserialize(source);
            return new AckableMessage<>(value, handle);
        }

        @Override
        public AckableMessage<T> deserialize(AckableMessage<T> reuse, DataInputView source)
                throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            serialize(deserialize(source), target);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Serializer<?> that = (Serializer<?>) obj;
            return valueSerializer.equals(that.valueSerializer)
                    && handleSerializer.equals(that.handleSerializer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(valueSerializer, handleSerializer);
        }

        @Override
        public TypeSerializerSnapshot<AckableMessage<T>> snapshotConfiguration() {
            return new SerializerSnapshot<>(this);
        }
    }

    /** Serializer snapshot that tracks the compatibility of the nested value serializer. */
    public static final class SerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<AckableMessage<T>, Serializer<T>> {

        private static final int CURRENT_VERSION = 1;

        public SerializerSnapshot() {}

        public SerializerSnapshot(Serializer<T> serializer) {
            super(serializer);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return CURRENT_VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(Serializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {
                outerSerializer.valueSerializer, outerSerializer.handleSerializer
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Serializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            return new Serializer<>(
                    (TypeSerializer<T>) nestedSerializers[0],
                    (TypeSerializer<RocketMQReceiptHandle>) nestedSerializers[1]);
        }
    }
}
