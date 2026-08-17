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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.common.ClientConfigurationProvider;
import org.apache.flink.connector.rocketmq.grpc.common.FilterExpressionParser;
import org.apache.flink.connector.rocketmq.grpc.source.ConsumerMode;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * A factory that builds a gRPC Pop consumer from a Flink configuration: a {@code
 * LiteSimpleConsumer} bound to the main lite topic in {@link ConsumerMode#LITE}, or a {@code
 * SimpleConsumer} subscribed to a normal topic with a filter expression in {@link
 * ConsumerMode#SIMPLE}.
 */
@Internal
public class PopConsumerProvider {

    private PopConsumerProvider() {}

    /**
     * Build and start the {@link PopConsumer} selected by {@link RocketMQGrpcSourceOptions#MODE}.
     */
    public static PopConsumer create(Configuration configuration) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        if (configuration.get(RocketMQGrpcSourceOptions.MODE) == ConsumerMode.SIMPLE) {
            final FilterExpression filter =
                    FilterExpressionParser.parse(
                            configuration.get(RocketMQGrpcSourceOptions.FILTER_EXPRESSION),
                            configuration.get(RocketMQGrpcSourceOptions.FILTER_TYPE));
            final SimpleConsumer consumer =
                    provider.newSimpleConsumerBuilder()
                            .setClientConfiguration(
                                    ClientConfigurationProvider.getClientConfiguration(
                                            configuration))
                            .setConsumerGroup(
                                    configuration.get(
                                            RocketMQGrpcSourceOptions.CONSUMER_GROUP))
                            .setAwaitDuration(
                                    configuration.get(RocketMQGrpcSourceOptions.AWAIT_DURATION))
                            .setSubscriptionExpressions(
                                    Collections.singletonMap(
                                            configuration.get(RocketMQGrpcSourceOptions.TOPIC),
                                            filter))
                            .build();
            return new SimplePopConsumer(consumer);
        }
        final LiteSimpleConsumer consumer =
                provider.newLiteSimpleConsumerBuilder()
                        .setClientConfiguration(
                                ClientConfigurationProvider.getClientConfiguration(configuration))
                        .setConsumerGroup(
                                configuration.get(
                                        RocketMQGrpcSourceOptions.CONSUMER_GROUP))
                        .setAwaitDuration(
                                configuration.get(RocketMQGrpcSourceOptions.AWAIT_DURATION))
                        .bindTopic(configuration.get(RocketMQGrpcSourceOptions.MAIN_TOPIC))
                        .build();
        return new LitePopConsumer(consumer);
    }

    /** Wrap an SDK {@code LiteSimpleConsumer} into the connector's {@link PopConsumer} surface. */
    @VisibleForTesting
    static PopConsumer wrap(LiteSimpleConsumer consumer) {
        return new LitePopConsumer(consumer);
    }

    /** Wrap an SDK {@code SimpleConsumer} into the connector's {@link PopConsumer} surface. */
    @VisibleForTesting
    static PopConsumer wrap(SimpleConsumer consumer) {
        return new SimplePopConsumer(consumer);
    }

    private static final class LitePopConsumer implements PopConsumer {

        private final LiteSimpleConsumer delegate;

        private LitePopConsumer(LiteSimpleConsumer delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
                throws ClientException {
            return delegate.receive(maxMessageNum, invisibleDuration);
        }

        @Override
        public void ack(MessageView messageView) throws ClientException {
            delegate.ack(messageView);
        }

        @Override
        public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
                throws ClientException {
            delegate.changeInvisibleDuration(messageView, invisibleDuration);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class SimplePopConsumer implements PopConsumer {

        private final SimpleConsumer delegate;

        private SimplePopConsumer(SimpleConsumer delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
                throws ClientException {
            return delegate.receive(maxMessageNum, invisibleDuration);
        }

        @Override
        public void ack(MessageView messageView) throws ClientException {
            delegate.ack(messageView);
        }

        @Override
        public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
                throws ClientException {
            delegate.changeInvisibleDuration(messageView, invisibleDuration);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
