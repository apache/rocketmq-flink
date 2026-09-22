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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the SIMPLE-mode acknowledgement path of {@link RocketMQGrpcSourceSplitReader}. */
class RocketMQGrpcSourceSplitReaderTest {

    @Test
    void ackBeforeStartThrowsTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        try {
            assertThatThrownBy(() -> reader.ack(handle()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not started yet");
        } finally {
            reader.close();
        }
    }

    @Test
    void ackAfterCloseThrowsTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final RecordingPopConsumer consumer = new RecordingPopConsumer();
        reader.setConsumer(consumer);
        reader.close();

        assertThatThrownBy(() -> reader.ack(handle()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already closed");
        assertThat(consumer.acked).isEmpty();
    }

    @Test
    void ackDelegatesRebuiltMessageViewToConsumerTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final RecordingPopConsumer consumer = new RecordingPopConsumer();
        reader.setConsumer(consumer);

        final RocketMQReceiptHandle handle = handle();
        try {
            reader.ack(handle);
        } finally {
            reader.close();
        }

        assertThat(consumer.acked).hasSize(1);
        final org.apache.rocketmq.client.java.message.MessageViewImpl acked =
                (org.apache.rocketmq.client.java.message.MessageViewImpl) consumer.acked.get(0);
        assertThat(acked.getMessageId())
                .isEqualTo(MessageIdCodec.getInstance().decode(handle.getMessageId()));
        assertThat(acked.getTopic()).isEqualTo(handle.getTopic());
        assertThat(acked.getReceiptHandle()).isEqualTo(handle.getReceiptHandle());
    }

    private static RocketMQReceiptHandle handle() {
        return new RocketMQReceiptHandle(
                "127.0.0.1:8081",
                "ns-1",
                "GID-test",
                "normal-topic",
                null,
                MessageIdCodec.getInstance().nextMessageId().toString(),
                "receipt-handle-abc",
                1);
    }

    private static final class RecordingPopConsumer implements PopConsumer {

        private final List<MessageView> acked = new ArrayList<>();

        @Override
        public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
                throws ClientException {
            return Collections.emptyList();
        }

        @Override
        public void ack(MessageView messageView) throws ClientException {
            acked.add(messageView);
        }

        @Override
        public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
                throws ClientException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {}
    }
}
