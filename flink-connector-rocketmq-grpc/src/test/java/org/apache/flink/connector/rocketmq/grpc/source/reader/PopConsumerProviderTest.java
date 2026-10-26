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

import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the {@link PopConsumer} adapters of {@link PopConsumerProvider} delegate to the
 * wrapped SDK consumer. Building a real consumer requires a live proxy, so the SDK interfaces are
 * stubbed with dynamic proxies that only record the invoked method and its arguments.
 */
class PopConsumerProviderTest {

    @Test
    void litePopConsumerDelegatesAckTest() throws Exception {
        final List<Invocation> invocations = new ArrayList<>();
        final PopConsumer consumer =
                PopConsumerProvider.wrap(recordingProxy(LiteSimpleConsumer.class, invocations));

        final MessageView messageView = messageView();
        consumer.ack(messageView);

        assertThat(invocations).hasSize(1);
        assertThat(invocations.get(0).method).isEqualTo("ack");
        assertThat(invocations.get(0).argument).isSameAs(messageView);
    }

    @Test
    void simplePopConsumerDelegatesAckTest() throws Exception {
        final List<Invocation> invocations = new ArrayList<>();
        final PopConsumer consumer =
                PopConsumerProvider.wrap(recordingProxy(SimpleConsumer.class, invocations));

        final MessageView messageView = messageView();
        consumer.ack(messageView);

        assertThat(invocations).hasSize(1);
        assertThat(invocations.get(0).method).isEqualTo("ack");
        assertThat(invocations.get(0).argument).isSameAs(messageView);
    }

    @Test
    void subscribeLiteTopicsRegistersEverySubTopicTest() throws Exception {
        final List<Invocation> invocations = new ArrayList<>();
        final LiteSimpleConsumer consumer = recordingProxy(LiteSimpleConsumer.class, invocations);

        PopConsumerProvider.subscribeLiteTopics(consumer, Arrays.asList("subA", "subB"));

        assertThat(invocations).hasSize(2);
        assertThat(invocations.get(0).method).isEqualTo("subscribeLite");
        assertThat(invocations.get(0).argument).isEqualTo("subA");
        assertThat(invocations.get(1).method).isEqualTo("subscribeLite");
        assertThat(invocations.get(1).argument).isEqualTo("subB");
    }

    @Test
    void subscribeLiteTopicsRejectsEmptySubscriptionTest() {
        final LiteSimpleConsumer consumer =
                recordingProxy(LiteSimpleConsumer.class, new ArrayList<>());

        assertThatThrownBy(
                        () ->
                                PopConsumerProvider.subscribeLiteTopics(
                                        consumer, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sub lite topic");
    }

    /**
     * A rejected lite subscription must not leak the already started consumer, otherwise its
     * channels and threads outlive the failed task.
     */
    @Test
    void subscribeOrCloseClosesConsumerWhenSubscriptionFailsTest() {
        final List<Invocation> invocations = new ArrayList<>();
        final LiteSimpleConsumer consumer = recordingProxy(LiteSimpleConsumer.class, invocations);

        assertThatThrownBy(
                        () ->
                                PopConsumerProvider.subscribeOrClose(
                                        consumer, Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sub lite topic");

        assertThat(invocations).hasSize(1);
        assertThat(invocations.get(0).method).isEqualTo("close");
    }

    @Test
    void subscribeOrCloseKeepsConsumerWhenSubscriptionSucceedsTest() throws Exception {
        final List<Invocation> invocations = new ArrayList<>();
        final LiteSimpleConsumer consumer = recordingProxy(LiteSimpleConsumer.class, invocations);

        PopConsumerProvider.subscribeOrClose(consumer, Arrays.asList("subA", "subB"));

        assertThat(invocations).hasSize(2);
        assertThat(invocations.get(0).method).isEqualTo("subscribeLite");
        assertThat(invocations.get(1).method).isEqualTo("subscribeLite");
    }

    private static <T> T recordingProxy(Class<T> type, List<Invocation> invocations) {
        return type.cast(
                Proxy.newProxyInstance(
                        type.getClassLoader(),
                        new Class<?>[] {type},
                        (proxy, method, args) -> {
                            invocations.add(
                                    new Invocation(
                                            method.getName(), args == null ? null : args[0]));
                            return null;
                        }));
    }

    private static MessageView messageView() {
        return (MessageView)
                Proxy.newProxyInstance(
                        MessageView.class.getClassLoader(),
                        new Class<?>[] {MessageView.class},
                        (proxy, method, args) -> {
                            throw new UnsupportedOperationException();
                        });
    }

    private static final class Invocation {

        private final String method;
        private final Object argument;

        private Invocation(String method, Object argument) {
            this.method = method;
            this.argument = argument;
        }
    }
}
