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
import org.apache.flink.streaming.connectors.rocketmq.common.selector.HashMessageQueueSelector;
import org.apache.flink.streaming.connectors.rocketmq.common.selector.MessageQueueSelector;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a configured {@link MessageQueueSelector} actually reaches the producer. Accepting the
 * selector on the builder without ever passing it to the send call silently turns ordered writes
 * into round robin writes, which no amount of configuration can reveal at runtime.
 */
class RocketMQWriterQueueSelectorTest {

    private static final String TOPIC = "TopicWriterSelector";

    @Test
    void configuredSelectorReachesTheProducerTest() throws Exception {
        RecordingProducer producer = new RecordingProducer();
        MessageQueueSelector selector = new HashMessageQueueSelector();

        newWriter(selector, producer).write("sharding-key", null);

        assertThat(producer.plainSends).isEmpty();
        assertThat(producer.selectorSends).hasSize(1);
        assertThat(producer.selectorSends.get(0).selector).isSameAs(selector);
        assertThat(producer.selectorSends.get(0).arg).isEqualTo("sharding-key");
        assertThat(producer.selectorSends.get(0).message.getTopic()).isEqualTo(TOPIC);
    }

    @Test
    void withoutSelectorThePlainSendIsUsedTest() throws Exception {
        RecordingProducer producer = new RecordingProducer();

        newWriter(null, producer).write("sharding-key", null);

        assertThat(producer.plainSends).hasSize(1);
        assertThat(producer.selectorSends).isEmpty();
    }

    private static RocketMQWriter<String> newWriter(
            MessageQueueSelector selector, InnerProducer producer) {
        Configuration configuration = new Configuration();
        configuration.setString(
                RocketMQSinkOptions.DELIVERY_GUARANTEE, DeliveryGuarantee.AT_LEAST_ONCE.name());
        return new RocketMQWriter<>(
                configuration,
                selector,
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

    /** Records which send overload the writer chose. */
    private static class RecordingProducer implements InnerProducer {

        private final List<Message> plainSends = new ArrayList<>();
        private final List<SelectorSend> selectorSends = new ArrayList<>();

        @Override
        public void start() {}

        @Override
        public String getProducerGroup() {
            return "recording-producer";
        }

        @Override
        public CompletableFuture<SendResult> send(Message message) {
            plainSends.add(message);
            return CompletableFuture.completedFuture(new SendResult());
        }

        @Override
        public CompletableFuture<SendResult> send(
                Message message, MessageQueueSelector selector, Object arg) {
            selectorSends.add(new SelectorSend(message, selector, arg));
            return CompletableFuture.completedFuture(new SendResult());
        }

        @Override
        public CompletableFuture<SendResult> sendMessageInTransaction(Message message) {
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

        @Override
        public void close() {}

        private static class SelectorSend {
            private final Message message;
            private final MessageQueueSelector selector;
            private final Object arg;

            private SelectorSend(Message message, MessageQueueSelector selector, Object arg) {
                this.message = message;
                this.selector = selector;
                this.arg = arg;
            }
        }
    }
}
