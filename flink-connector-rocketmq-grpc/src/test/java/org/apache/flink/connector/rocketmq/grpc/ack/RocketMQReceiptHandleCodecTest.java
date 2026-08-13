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

import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RocketMQReceiptHandleCodec}. */
class RocketMQReceiptHandleCodecTest {

    private static RocketMQReceiptHandle newHandle(String liteTopic) {
        final String messageId = MessageIdCodec.getInstance().nextMessageId().toString();
        return new RocketMQReceiptHandle(
                "127.0.0.1:8080",
                "ns-1",
                "GID-test",
                "main-topic",
                liteTopic,
                messageId,
                "receipt-handle-abc",
                2);
    }

    @Test
    void toAckableThenExtractRoundTripsAllFields() {
        final RocketMQReceiptHandle original = newHandle("sub-topic-a");

        final MessageView view = RocketMQReceiptHandleCodec.toAckable(original);
        final RocketMQReceiptHandle roundTripped =
                RocketMQReceiptHandleCodec.extract(
                        view, original.getNamespace(), original.getConsumerGroup());

        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void toAckableSupportsNullLiteTopic() {
        final RocketMQReceiptHandle original = newHandle(null);

        final MessageView view = RocketMQReceiptHandleCodec.toAckable(original);
        final RocketMQReceiptHandle roundTripped =
                RocketMQReceiptHandleCodec.extract(
                        view, original.getNamespace(), original.getConsumerGroup());

        assertThat(roundTripped.getLiteTopic()).isNull();
        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void reconstructedViewCarriesParseableMessageIdAndEndpoints() {
        final RocketMQReceiptHandle original = newHandle("sub-topic-a");

        final MessageViewImpl view =
                (MessageViewImpl) RocketMQReceiptHandleCodec.toAckable(original);

        // MessageId round-trips through the SDK codec.
        final MessageId expectedId = MessageIdCodec.getInstance().decode(original.getMessageId());
        assertThat(view.getMessageId()).isEqualTo(expectedId);

        // Endpoints round-trip through the string form used by the handle.
        final Endpoints endpoints = view.getEndpoints();
        assertThat(endpoints).isNotNull();
        assertThat(endpoints).isEqualTo(new Endpoints(original.getEndpoint()));

        assertThat(view.getTopic()).isEqualTo(original.getTopic());
        assertThat(view.getReceiptHandle()).isEqualTo(original.getReceiptHandle());
        assertThat(view.getDeliveryAttempt()).isEqualTo(original.getDeliveryAttempt());
    }

    @Test
    void extractRejectsUnexpectedMessageViewType() {
        final MessageView notImpl =
                (MessageView)
                        Proxy.newProxyInstance(
                                MessageView.class.getClassLoader(),
                                new Class[] {MessageView.class},
                                (proxy, method, args) -> {
                                    throw new UnsupportedOperationException();
                                });

        assertThatThrownBy(() -> RocketMQReceiptHandleCodec.extract(notImpl, "ns", "grp"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("internal message type");
    }
}
