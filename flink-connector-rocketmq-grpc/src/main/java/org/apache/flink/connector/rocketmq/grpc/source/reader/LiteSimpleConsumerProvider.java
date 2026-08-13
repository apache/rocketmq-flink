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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.common.ClientConfigurationProvider;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;

/**
 * A factory that builds a gRPC {@link LiteSimpleConsumer} (Pop model) from a Flink configuration.
 *
 * <p>The consumer binds the configured main topic and relies on its wildcard (generalized)
 * subscription to receive messages from all of its sub topics; the consumer group must carry the
 * {@code lite.sub.wildcard=true} attribute on the broker.
 */
@Internal
public class LiteSimpleConsumerProvider {

    private LiteSimpleConsumerProvider() {}

    /**
     * Build and start a {@link LiteSimpleConsumer} from the given Flink configuration.
     *
     * @param configuration the Flink configuration.
     * @return a started {@link LiteSimpleConsumer}.
     */
    public static LiteSimpleConsumer create(Configuration configuration) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        return provider.newLiteSimpleConsumerBuilder()
                .setClientConfiguration(
                        ClientConfigurationProvider.getClientConfiguration(configuration))
                .setConsumerGroup(configuration.get(RocketMQGrpcSourceOptions.CONSUMER_GROUP))
                .setAwaitDuration(configuration.get(RocketMQGrpcSourceOptions.AWAIT_DURATION))
                .bindTopic(configuration.get(RocketMQGrpcSourceOptions.MAIN_TOPIC))
                .build();
    }
}
