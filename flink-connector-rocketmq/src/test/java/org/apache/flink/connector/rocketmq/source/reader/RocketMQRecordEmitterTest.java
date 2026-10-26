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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQSchemaWrapper;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplitState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Test for {@link RocketMQRecordEmitter}. */
public class RocketMQRecordEmitterTest {

    @Test
    public void testEmitRecord() throws IOException {
        RocketMQRecordEmitter<RowData> recordEmitter =
                new RocketMQRecordEmitter<>(new RocketMQSchemaWrapper<>());
        MessageExt message =
                new MessageExt(
                        1,
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8080),
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8088),
                        "184019387");
        message.setBody("test_emit_record_message".getBytes());
        GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, message.getBody());
        String topic = "test-record-emitter";
        String broker = "taobaodaily";
        int partition = 256;
        long startingOffset = 100;
        long stoppingTimestamp = System.currentTimeMillis();

        MessageView messageView = new MessageViewExt(message);
        RocketMQSourceSplitState partitionSplitState =
                new RocketMQSourceSplitState(
                        new RocketMQSourceSplit(topic, broker, partition, startingOffset, -1L));

        recordEmitter.emitRecord(messageView, new TestingEmitterOutput<>(), partitionSplitState);
    }

    /**
     * A record whose deserialization throws must be dropped with a warning instead of failing the
     * task: otherwise the job loops "fail, restore, re-read the same record" and rewrites everything
     * it already emitted. The split offset still has to advance past the poison record.
     */
    @Test
    public void testUndeserializableRecordIsDroppedAndOffsetAdvances() throws IOException {
        RocketMQRecordEmitter<RowData> recordEmitter =
                new RocketMQRecordEmitter<>(new ThrowingSchemaWrapper<>());
        MessageExt message =
                new MessageExt(
                        1,
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8080),
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8088),
                        "184019388");
        message.setBody("poison".getBytes());
        message.setQueueOffset(100L);

        RocketMQSourceSplitState splitState =
                new RocketMQSourceSplitState(
                        new RocketMQSourceSplit("t", "b", 0, 100L, -1L));

        recordEmitter.emitRecord(new MessageViewExt(message), new TestingEmitterOutput<>(), splitState);

        assertEquals(101L, splitState.getCurrentOffset());
    }

    /**
     * A failure raised while emitting an already-deserialized record (the downstream sink runs
     * inline inside collect) must still fail the task: dropping it would hide a real write failure.
     */
    @Test
    public void testSinkFailureDuringCollectIsNotSwallowed() {
        RocketMQRecordEmitter<RowData> recordEmitter =
                new RocketMQRecordEmitter<>(new CollectingSchemaWrapper<>());
        MessageExt message =
                new MessageExt(
                        1,
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8080),
                        System.currentTimeMillis(),
                        InetSocketAddress.createUnresolved("127.0.0.1", 8088),
                        "184019389");
        message.setBody("fine".getBytes());
        message.setQueueOffset(7L);

        RocketMQSourceSplitState splitState =
                new RocketMQSourceSplitState(new RocketMQSourceSplit("t", "b", 0, 7L, -1L));

        try {
            recordEmitter.emitRecord(
                    new MessageViewExt(message), new FailingEmitterOutput<>(), splitState);
            fail("a sink failure during collect must propagate");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("Failed to emit message"));
        }
    }

    /** Emits exactly one record, so the output's failure happens inside collect. */
    private static final class CollectingSchemaWrapper<T> extends RocketMQSchemaWrapper<T> {
        @Override
        public void deserialize(MessageView messageView, Collector<T> out) {
            out.collect(null);
        }
    }

    private static final class FailingEmitterOutput<E> extends TestingEmitterOutput<E> {
        @Override
        public void collect(E record) {
            throw new RuntimeException("sink rejected the record");
        }
    }

    /** Deserialization always fails, as a schema rejecting an unparsable body would. */
    private static final class ThrowingSchemaWrapper<T> extends RocketMQSchemaWrapper<T> {
        @Override
        public void deserialize(MessageView messageView, Collector<T> out) throws IOException {
            throw new IOException("deliberately unparsable body");
        }
    }

    private static class TestingEmitterOutput<E> implements ReaderOutput<E> {

        private TestingEmitterOutput() {}

        public void collect(E record) {}

        public void collect(E record, long timestamp) {
            this.collect(record);
        }

        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {}

        public SourceOutput<E> createOutputForSplit(String splitId) {
            return this;
        }

        public void releaseOutputForSplit(String splitId) {}
    }
}
