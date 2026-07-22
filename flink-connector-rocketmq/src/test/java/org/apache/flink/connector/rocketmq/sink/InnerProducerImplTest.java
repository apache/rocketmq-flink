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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.configuration.Configuration;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for verifying that configuration parameters are applied to the RocketMQ producer. */
class InnerProducerImplTest {

    private TransactionMQProducer getProducerViaReflection(InnerProducerImpl impl) throws Exception {
        Field field = InnerProducerImpl.class.getDeclaredField("producer");
        field.setAccessible(true);
        return (TransactionMQProducer) field.get(impl);
    }

    @Test
    void sendTimeoutShouldBeAppliedToProducer() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSinkOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSinkOptions.PRODUCER_GROUP, "test-group");
        config.setLong(RocketMQSinkOptions.SEND_TIMEOUT, 8000L);

        try (InnerProducerImpl impl = new InnerProducerImpl(config)) {
            TransactionMQProducer producer = getProducerViaReflection(impl);
            assertThat(producer.getSendMsgTimeout()).isEqualTo(8000);
        }
    }

    @Test
    void sendRetryTimesShouldBeAppliedToProducer() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSinkOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSinkOptions.PRODUCER_GROUP, "test-group");
        config.setInteger(RocketMQSinkOptions.SEND_RETRY_TIMES, 5);

        try (InnerProducerImpl impl = new InnerProducerImpl(config)) {
            TransactionMQProducer producer = getProducerViaReflection(impl);
            assertThat(producer.getRetryTimesWhenSendFailed()).isEqualTo(5);
            assertThat(producer.getRetryTimesWhenSendAsyncFailed()).isEqualTo(5);
        }
    }

    @Test
    void defaultSendTimeoutShouldBe5000ms() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSinkOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSinkOptions.PRODUCER_GROUP, "test-group");

        try (InnerProducerImpl impl = new InnerProducerImpl(config)) {
            TransactionMQProducer producer = getProducerViaReflection(impl);
            assertThat(producer.getSendMsgTimeout()).isEqualTo(5000);
        }
    }

    @Test
    void defaultRetryTimesShouldBe3() throws Exception {
        Configuration config = new Configuration();
        config.setString(RocketMQSinkOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSinkOptions.PRODUCER_GROUP, "test-group");

        try (InnerProducerImpl impl = new InnerProducerImpl(config)) {
            TransactionMQProducer producer = getProducerViaReflection(impl);
            assertThat(producer.getRetryTimesWhenSendFailed()).isEqualTo(3);
            assertThat(producer.getRetryTimesWhenSendAsyncFailed()).isEqualTo(3);
        }
    }
}
