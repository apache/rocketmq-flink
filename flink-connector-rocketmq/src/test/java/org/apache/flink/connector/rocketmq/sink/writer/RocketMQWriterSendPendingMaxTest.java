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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        producer.completeAll();
        writer.close();
    }

    @Test
    @Timeout(10)
    void failedCallbackUnblocksAndFailsWaitingWriteTest() throws Exception {
        GatedProducer producer = new GatedProducer();
        RocketMQWriter<String> writer = newWriter(producer, 1);
        writer.write("first", null);
        java.util.concurrent.atomic.AtomicReference<Throwable> error =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread waiting =
                new Thread(
                        () -> {
                            try {
                                writer.write("second", null);
                            } catch (Exception e) {
                                error.set(e);
                            }
                        });
        waiting.start();
        waiting.join(100);
        assertThat(waiting.isAlive()).isTrue();
        producer.sends.get(0).completeExceptionally(new IllegalStateException("send failed"));
        waiting.join(2000);
        assertThat(waiting.isAlive()).isFalse();
        assertThat(error.get()).isInstanceOf(java.io.IOException.class);
        assertThat(producer.sends).hasSize(1);
        assertThatThrownBy(() -> writer.flush(false)).isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(() -> writer.write("third", null))
                .isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(writer::close).isInstanceOf(java.io.IOException.class);
    }

    @Test
    @Timeout(10)
    void flushWaitsForAllCallbacksTest() throws Exception {
        GatedProducer producer = new GatedProducer();
        RocketMQWriter<String> writer = newWriter(producer, 2);
        writer.write("first", null);
        writer.write("second", null);
        java.util.concurrent.atomic.AtomicBoolean finished =
                new java.util.concurrent.atomic.AtomicBoolean();
        Thread flush =
                new Thread(
                        () -> {
                            try {
                                writer.flush(false);
                                finished.set(true);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        flush.start();
        flush.join(100);
        assertThat(finished.get()).isFalse();
        producer.sends.get(1).complete(new SendResult());
        flush.join(100);
        assertThat(finished.get()).isFalse();
        producer.sends.get(0).complete(new SendResult());
        flush.join(2000);
        assertThat(finished.get()).isTrue();
        writer.close();
    }

    @Test
    void submissionExceptionReturnsPermitTest() throws Exception {
        InnerProducer producer = Mockito.mock(InnerProducer.class);
        Mockito.when(producer.send(Mockito.any(Message.class)))
                .thenThrow(new IllegalStateException("submission rejected"))
                .thenReturn(CompletableFuture.completedFuture(new SendResult()));
        Configuration config = new Configuration();
        config.setInteger(RocketMQSinkOptions.SEND_PENDING_MAX, 1);
        Sink.InitContext context = Mockito.mock(Sink.InitContext.class);
        Mockito.when(context.metricGroup())
                .thenReturn(
                        org.apache.flink.metrics.groups.UnregisteredMetricsGroup
                                .createSinkWriterMetricGroup());
        RocketMQWriter<String> writer =
                new RocketMQWriter<>(config, null, new StringToMessageSchema(), context, producer);
        assertThatThrownBy(() -> writer.write("first", null))
                .isInstanceOf(IllegalStateException.class);
        writer.write("second", null);
        writer.flush(false);
        writer.close();
        Mockito.verify(producer, Mockito.times(2)).send(Mockito.any(Message.class));
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
        Sink.InitContext initContext = Mockito.mock(Sink.InitContext.class);
        Mockito.when(initContext.metricGroup())
                .thenReturn(
                        org.apache.flink.metrics.groups.UnregisteredMetricsGroup
                                .createSinkWriterMetricGroup());
        return new RocketMQWriter<>(
                configuration, null, new StringToMessageSchema(), initContext, producer);
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
