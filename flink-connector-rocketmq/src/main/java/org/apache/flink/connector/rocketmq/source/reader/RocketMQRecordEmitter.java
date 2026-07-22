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

import java.io.IOException;

/** The {@link RecordEmitter} implementation for {@link RocketMQSourceReader}. */
public class RocketMQRecordEmitter<T>
        implements RecordEmitter<MessageView, T, RocketMQSourceSplitState> {

    private final RocketMQDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public RocketMQRecordEmitter(RocketMQDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            MessageView element, SourceOutput<T> output, RocketMQSourceSplitState splitState)
            throws IOException {

        try {
            sourceOutputWrapper.setSourceOutput(output);
            sourceOutputWrapper.setTimestamp(element.getEventTime());
            deserializationSchema.deserialize(element, sourceOutputWrapper);
            splitState.setCurrentOffset(element.getQueueOffset() + 1);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize message due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;

        private long timestamp;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
