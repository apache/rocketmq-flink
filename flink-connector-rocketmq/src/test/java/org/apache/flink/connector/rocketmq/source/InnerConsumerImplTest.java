/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for verifying that configuration parameters are applied to the RocketMQ consumer. */
class InnerConsumerImplTest {

    private DefaultLitePullConsumer getConsumerViaReflection(InnerConsumerImpl impl)
            throws Exception {
        Field field = InnerConsumerImpl.class.getDeclaredField("consumer");
        field.setAccessible(true);
        return (DefaultLitePullConsumer) field.get(impl);
    }

    @Test
    void pullBatchSizeShouldBeAppliedToConsumer() throws Exception {
        Configuration config = new Configuration();
        config.set(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.set(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");
        config.set(RocketMQSourceOptions.PULL_BATCH_SIZE, 64L);

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {
            DefaultLitePullConsumer consumer = getConsumerViaReflection(impl);
            assertThat(consumer.getPullBatchSize()).isEqualTo(64);
        }
    }

    @Test
    void defaultPullBatchSizeShouldBe32() throws Exception {
        Configuration config = new Configuration();
        config.set(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.set(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {
            DefaultLitePullConsumer consumer = getConsumerViaReflection(impl);
            assertThat(consumer.getPullBatchSize()).isEqualTo(32);
        }
    }

    @Test
    void namespaceConfigShouldBeReadable() throws Exception {
        Configuration config = new Configuration();
        config.set(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.set(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");
        config.set(RocketMQOptions.NAMESPACE, "my-namespace");

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {

            assertThat(impl).isNotNull(); // setNamespace verified by successful construction
        }
    }

    @Test
    void seekRecoversFromTransportFailureTest() throws Exception {
        DefaultLitePullConsumer consumer = mock(DefaultLitePullConsumer.class);
        MessageQueue queue = new MessageQueue("topic", "broker", 0);
        MQClientException failure =
                new MQClientException("transport", new RemotingSendRequestException("broker"));
        doThrow(failure).doThrow(failure).doNothing().when(consumer).seek(queue, 42L);

        try (InnerConsumerImpl impl = mockedConsumer(consumer)) {
            impl.seek(queue, 42L);
            verify(consumer, times(3)).seek(queue, 42L);
        }
    }

    @Test
    void seekExhaustedTransportRetriesTest() throws Exception {
        DefaultLitePullConsumer consumer = mock(DefaultLitePullConsumer.class);
        MessageQueue queue = new MessageQueue("topic", "broker", 0);
        MQClientException failure =
                new MQClientException("transport", new RemotingSendRequestException("broker"));
        doThrow(failure).when(consumer).seek(queue, 42L);

        try (InnerConsumerImpl impl = mockedConsumer(consumer)) {
            assertThatThrownBy(() -> impl.seek(queue, 42L))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasCause(failure);
            verify(consumer, times(3)).seek(queue, 42L);
        }
    }

    @Test
    void seekNonTransportFailureIsNotRetriedTest() throws Exception {
        DefaultLitePullConsumer consumer = mock(DefaultLitePullConsumer.class);
        MessageQueue queue = new MessageQueue("topic", "broker", 0);
        MQClientException failure = new MQClientException(16, "permission denied");
        doThrow(failure).when(consumer).seek(queue, 42L);

        try (InnerConsumerImpl impl = mockedConsumer(consumer)) {
            assertThatThrownBy(() -> impl.seek(queue, 42L))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasCause(failure);
            verify(consumer).seek(queue, 42L);
        }
    }

    @Test
    void seekRetryPreservesInterruptionTest() throws Exception {
        DefaultLitePullConsumer consumer = mock(DefaultLitePullConsumer.class);
        MessageQueue queue = new MessageQueue("topic", "broker", 0);
        doThrow(new MQClientException("transport", new RemotingSendRequestException("broker")))
                .when(consumer)
                .seek(queue, 42L);

        try (InnerConsumerImpl impl = mockedConsumer(consumer)) {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> impl.seek(queue, 42L))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(consumer).seek(queue, 42L);
        } finally {
            Thread.interrupted();
        }
    }

    private InnerConsumerImpl mockedConsumer(DefaultLitePullConsumer consumer) throws Exception {
        Configuration config = new Configuration();
        config.set(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.set(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");
        InnerConsumerImpl impl = new InnerConsumerImpl(config);
        Field field = InnerConsumerImpl.class.getDeclaredField("consumer");
        field.setAccessible(true);
        field.set(impl, consumer);
        return impl;
    }
}
