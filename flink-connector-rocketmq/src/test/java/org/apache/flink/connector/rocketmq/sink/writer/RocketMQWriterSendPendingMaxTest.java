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

package org.apache.flink.connector.rocketmq.sink.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.sink.InnerProducer;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.streaming.connectors.rocketmq.common.selector.MessageQueueSelector;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@code send.pending.max} bounds the in-flight async sends. The option ships a
 * description promising exactly that ("Maximum number of pending messages in the send buffer") but
 * was never read, so a slow broker let the buffer grow without limit.
 */
class RocketMQWriterSendPendingMaxTest {

    private static final String TOPIC = "TopicWriterPending";

    @Test
    @Timeout(30)
    void writesBlockOnceThePendingLimitIsReachedTest() throws Exception {
        GatedProducer producer = new GatedProducer();
        RocketMQWriter<String> writer = newWriter(producer, 1);

        writer.write("first", null);
        assertThat(producer.sends).hasSize(1);

        Thread second = new Thread(() -> uncheckedWrite(writer, "second"));
        second.start();
        second.join(1000);
        assertThat(second.isAlive())
                .as("the second write must block while the first send is still in flight")
                .isTrue();
        assertThat(producer.sends).hasSize(1);

        producer.completeAll();
        second.join(5000);
        assertThat(second.isAlive()).as("releasing the send must unblock the writer").isFalse();
        assertThat(producer.sends).hasSize(2);
        writer.close();
    }

    private static void uncheckedWrite(RocketMQWriter<String> writer, String record) {
        try {
            writer.write(record, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RocketMQWriter<String> newWriter(GatedProducer producer, int pendingMax) {
        Configuration configuration = new Configuration();
        configuration.setString(
                RocketMQSinkOptions.DELIVERY_GUARANTEE, DeliveryGuarantee.AT_LEAST_ONCE.name());
        configuration.setInteger(RocketMQSinkOptions.SEND_PENDING_MAX, pendingMax);
        return new RocketMQWriter<>(
                configuration,
                null,
                new StringToMessageSchema(),
                Mockito.mock(Sink.InitContext.class),
                producer);
    }

    /** Wraps the record into a message body so the send call can be inspected. */
    private static class StringToMessageSchema implements RocketMQSerializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public Message serialize(String element, RocketMQSinkContext context, Long timestamp) {
            return new Message(TOPIC, element.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Keeps every send future open until the test releases them. */
    private static class GatedProducer implements InnerProducer {

        private final List<CompletableFuture<SendResult>> sends = new ArrayList<>();

        @Override
        public void start() {}

        @Override
        public String getProducerGroup() {
            return "gated-producer";
        }

        @Override
        public synchronized CompletableFuture<SendResult> send(Message message) {
            CompletableFuture<SendResult> future = new CompletableFuture<>();
            sends.add(future);
            return future;
        }

        @Override
        public CompletableFuture<SendResult> send(
                Message message, MessageQueueSelector selector, Object arg) {
            return send(message);
        }

        @Override
        public CompletableFuture<SendResult> sendMessageInTransaction(Message message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String resolvePublishAddress(SendResult sendResult) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> commit(SendCommittable sendCommittable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> rollback(SendCommittable sendCommittable) {
            throw new UnsupportedOperationException();
        }

        synchronized void completeAll() {
            for (CompletableFuture<SendResult> future : sends) {
                future.complete(new SendResult());
            }
        }

        @Override
        public void close() {}
    }
}
