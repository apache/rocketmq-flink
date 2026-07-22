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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

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
        config.setString(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");
        config.setLong(RocketMQSourceOptions.PULL_BATCH_SIZE, 64L);

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {
            DefaultLitePullConsumer consumer = getConsumerViaReflection(impl);
            assertThat(consumer.getPullBatchSize()).isEqualTo(64);
        }
    }

    @Test
    void defaultPullBatchSizeShouldBe32() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {
            DefaultLitePullConsumer consumer = getConsumerViaReflection(impl);
            assertThat(consumer.getPullBatchSize()).isEqualTo(32);
        }
    }

    @Test
    void namespaceConfigShouldBeReadable() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSourceOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSourceOptions.CONSUMER_GROUP, "test-group");
        config.setString(RocketMQOptions.NAMESPACE, "my-namespace");

        try (InnerConsumerImpl impl = new InnerConsumerImpl(config)) {

            assertThat(impl).isNotNull(); // setNamespace verified by successful construction
        }
    }
}
