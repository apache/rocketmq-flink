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

package org.apache.flink.connector.rocketmq.grpc.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessage;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandleCodec;
import org.apache.flink.connector.rocketmq.grpc.source.deserialization.RocketMQGrpcDeserializationSchema;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplitState;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * The {@link RecordEmitter} implementation for the RocketMQ gRPC source. It deserializes the
 * message body into the value type {@code T}, extracts the credential-free receipt handle from the
 * SDK message view and emits an {@link AckableMessage} pairing the two so that a downstream
 * operator can acknowledge the message.
 */
@Internal
public class RocketMQGrpcSourceRecordEmitter<T>
        implements RecordEmitter<MessageViewImpl, AckableMessage<T>, RocketMQGrpcSourceSplitState> {

    private final RocketMQGrpcDeserializationSchema<T> deserializationSchema;
    private final String namespace;
    private final String consumerGroup;
    @Nullable private final Consumer<RocketMQReceiptHandle> emittedHandleListener;
    private final AckableCollector<T> collector = new AckableCollector<>();

    public RocketMQGrpcSourceRecordEmitter(
            RocketMQGrpcDeserializationSchema<T> deserializationSchema,
            String namespace,
            String consumerGroup) {
        this(deserializationSchema, namespace, consumerGroup, null);
    }

    public RocketMQGrpcSourceRecordEmitter(
            RocketMQGrpcDeserializationSchema<T> deserializationSchema,
            String namespace,
            String consumerGroup,
            @Nullable Consumer<RocketMQReceiptHandle> emittedHandleListener) {
        this.deserializationSchema = deserializationSchema;
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        this.emittedHandleListener = emittedHandleListener;
    }

    @Override
    public void emitRecord(
            MessageViewImpl element,
            SourceOutput<AckableMessage<T>> output,
            RocketMQGrpcSourceSplitState splitState)
            throws IOException {
        try {
            final RocketMQReceiptHandle handle =
                    RocketMQReceiptHandleCodec.extract(
                            element.getMessageView(), namespace, consumerGroup);
            collector.reset(output, element.getEventTime(), handle);
            deserializationSchema.deserialize(element, collector);
            if (emittedHandleListener != null) {
                emittedHandleListener.accept(handle);
            }
            splitState.incrementProcessedRecords();
        } catch (Exception e) {
            throw new IOException("Failed to deserialize message due to", e);
        }
    }

    /** A collector that wraps each deserialized value into an {@link AckableMessage}. */
    private static class AckableCollector<T> implements Collector<T> {

        private SourceOutput<AckableMessage<T>> sourceOutput;
        private long timestamp;
        private RocketMQReceiptHandle handle;

        @Override
        public void collect(T record) {
            sourceOutput.collect(new AckableMessage<>(record, handle), timestamp);
        }

        @Override
        public void close() {}

        private void reset(
                SourceOutput<AckableMessage<T>> sourceOutput,
                long timestamp,
                RocketMQReceiptHandle handle) {
            this.sourceOutput = sourceOutput;
            this.timestamp = timestamp;
            this.handle = handle;
        }
    }
}
