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
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a transactional write records the address the half message was accepted by. The
 * committer runs on a producer of its own, whose publish route is resolved independently and may
 * name a different node; a commit sent there is discarded while still answered with a success code.
 */
class RocketMQWriterTransactionCommittableTest {

    private static final String TOPIC = "TopicWriterTransaction";
    private static final String SEND_ADDRESS = "10.0.38.29:8080";

    @Test
    void prepareCommitCarriesTheAddressOfTheSendingNodeTest() throws Exception {
        TransactionalProducer producer = new TransactionalProducer();
        RocketMQWriter<String> writer = newWriter(producer);

        writer.write("record", null);
        Collection<SendCommittable> committables = writer.prepareCommit();

        assertThat(committables).hasSize(1);
        SendCommittable committable = committables.iterator().next();
        assertThat(committable.getBrokerAddress()).isEqualTo(SEND_ADDRESS);
        assertThat(committable.getTransactionId()).isEqualTo("trans-1");
        assertThat(committable.getBrokerName()).isEqualTo("broker-a");
        // The map must be drained, otherwise the next checkpoint commits the same half message
        // twice.
        assertThat(writer.prepareCommit()).isEmpty();
    }

    private static RocketMQWriter<String> newWriter(InnerProducer producer) {
        Configuration configuration = new Configuration();
        configuration.setString(
                RocketMQSinkOptions.DELIVERY_GUARANTEE, DeliveryGuarantee.EXACTLY_ONCE.name());
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

    /** Returns a send result plus the address the writer is expected to record for it. */
    private static class TransactionalProducer implements InnerProducer {

        @Override
        public void start() {}

        @Override
        public String getProducerGroup() {
            return "transactional-producer";
        }

        @Override
        public CompletableFuture<SendResult> send(Message message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<SendResult> send(
                Message message, MessageQueueSelector selector, Object arg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<SendResult> sendMessageInTransaction(Message message) {
            SendResult sendResult = new SendResult();
            sendResult.setSendStatus(SendStatus.SEND_OK);
            sendResult.setMessageQueue(new MessageQueue(TOPIC, "broker-a", 3));
            sendResult.setQueueOffset(42L);
            sendResult.setMsgId("msg-1");
            sendResult.setOffsetMsgId("0A00000100002382000000000000002A");
            sendResult.setTransactionId("trans-1");
            return CompletableFuture.completedFuture(sendResult);
        }

        @Override
        public String resolvePublishAddress(SendResult sendResult) {
            return SEND_ADDRESS;
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
    }
}
