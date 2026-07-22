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

import org.apache.rocketmq.client.apis.message.MessageId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.FutureTask;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the renewal/emit bookkeeping of {@link MessageViewImpl}. */
class MessageViewImplTest {

    @Test
    void markEmittedCancelsPendingRenewalTest() {
        final MessageViewImpl view = new MessageViewImpl(new TestingSdkMessageView());
        final FutureTask<Void> future = new FutureTask<>(() -> null);
        view.setRenewalFuture(future);

        assertThat(view.isEmitted()).isFalse();
        view.markEmitted();

        assertThat(view.isEmitted()).isTrue();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void renewalFutureSetAfterEmitIsCancelledTest() {
        final MessageViewImpl view = new MessageViewImpl(new TestingSdkMessageView());
        view.markEmitted();

        final FutureTask<Void> future = new FutureTask<>(() -> null);
        view.setRenewalFuture(future);

        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    void copiesBodyAndExposesAttributesTest() {
        final MessageViewImpl view = new MessageViewImpl(new TestingSdkMessageView());

        assertThat(view.getBody()).isEqualTo("body".getBytes(StandardCharsets.UTF_8));
        assertThat(view.getTopic()).isEqualTo("topic");
        assertThat(view.getTag()).isNull();
        assertThat(view.getDeliveryAttempt()).isEqualTo(2);
        assertThat(view.getEventTime()).isEqualTo(1234L);
    }

    /** A minimal SDK message view stub. */
    private static class TestingSdkMessageView
            implements org.apache.rocketmq.client.apis.message.MessageView {

        @Override
        public MessageId getMessageId() {
            return null;
        }

        @Override
        public String getTopic() {
            return "topic";
        }

        @Override
        public ByteBuffer getBody() {
            return ByteBuffer.wrap("body".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Map<String, String> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Optional<String> getTag() {
            return Optional.empty();
        }

        @Override
        public Collection<String> getKeys() {
            return Collections.emptyList();
        }

        @Override
        public Optional<String> getMessageGroup() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getLiteTopic() {
            return Optional.empty();
        }

        @Override
        public Optional<Long> getDeliveryTimestamp() {
            return Optional.empty();
        }

        @Override
        public Optional<Integer> getPriority() {
            return Optional.empty();
        }

        @Override
        public String getBornHost() {
            return "localhost";
        }

        @Override
        public long getBornTimestamp() {
            return 1234L;
        }

        @Override
        public int getDeliveryAttempt() {
            return 2;
        }
    }
}
