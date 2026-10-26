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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplitState;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** The {@link RecordEmitter} implementation for {@link RocketMQSourceReader}. */
public class RocketMQRecordEmitter<T>
        implements RecordEmitter<MessageView, T, RocketMQSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQRecordEmitter.class);

    private final RocketMQDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public RocketMQRecordEmitter(RocketMQDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            MessageView element, SourceOutput<T> output, RocketMQSourceSplitState splitState)
            throws IOException {

        sourceOutputWrapper.setSourceOutput(output);
        sourceOutputWrapper.setTimestamp(element.getEventTime());
        sourceOutputWrapper.resetCollectError();
        try {
            deserializationSchema.deserialize(element, sourceOutputWrapper);
        } catch (Exception e) {
            RuntimeException collectError = sourceOutputWrapper.getCollectError();
            if (collectError != null) {
                // The record deserialized fine; the failure came from the downstream sink while the
                // already-deserialized record was being emitted (collect runs the writer inline).
                // Dropping it here would hide a real write failure behind a "deserialization" warn.
                throw new IOException(
                        String.format(
                                "Failed to emit message %s from topic %s, queue %d, offset %d",
                                element.getMessageId(),
                                element.getTopic(),
                                element.getQueueId(),
                                element.getQueueOffset()),
                        collectError);
            }
            // A record that cannot be deserialized is dropped instead of failing the task: with a
            // throwing schema the job would otherwise loop "fail, restore, re-read the same record"
            // forever and rewrite everything it had already emitted. Naming the record keeps the
            // poison message identifiable from the log line alone.
            LOG.warn(
                    "Dropped message {} from topic {}, queue {}, offset {} because deserialization failed",
                    element.getMessageId(),
                    element.getTopic(),
                    element.getQueueId(),
                    element.getQueueOffset(),
                    e);
        }
        splitState.setCurrentOffset(element.getQueueOffset() + 1);
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;

        private long timestamp;

        private RuntimeException collectError;

        @Override
        public void collect(T record) {
            try {
                sourceOutput.collect(record, timestamp);
            } catch (RuntimeException e) {
                collectError = e;
                throw e;
            }
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        private void resetCollectError() {
            collectError = null;
        }

        private RuntimeException getCollectError() {
            return collectError;
        }
    }
}
