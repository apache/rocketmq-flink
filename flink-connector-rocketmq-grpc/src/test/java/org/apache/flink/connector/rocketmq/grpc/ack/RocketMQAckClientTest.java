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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the reference counting behaviour of {@link RocketMQAckClient#acquire}. */
class RocketMQAckClientTest {

    private static Configuration config(String endpoints) {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ENDPOINTS, endpoints);
        return configuration;
    }

    @Test
    void sameConfigurationSharesASingleReferenceCountedClient() {
        final Configuration configuration = config("127.0.0.1:8080");
        assertThat(RocketMQAckClient.getReferenceCount(configuration)).isZero();

        final RocketMQAckClient first = RocketMQAckClient.acquire(configuration);
        final RocketMQAckClient second = RocketMQAckClient.acquire(configuration);

        // The same client is shared and the reference count is incremented per acquire.
        assertThat(second).isSameAs(first);
        assertThat(RocketMQAckClient.getReferenceCount(configuration)).isEqualTo(2);

        RocketMQAckClient.release(configuration);
        assertThat(RocketMQAckClient.getReferenceCount(configuration)).isEqualTo(1);

        RocketMQAckClient.release(configuration);
        assertThat(RocketMQAckClient.getReferenceCount(configuration)).isZero();
    }

    @Test
    void differentConfigurationsGetDistinctClients() {
        final Configuration a = config("host-a:8080");
        final Configuration b = config("host-b:8080");

        final RocketMQAckClient clientA = RocketMQAckClient.acquire(a);
        final RocketMQAckClient clientB = RocketMQAckClient.acquire(b);
        try {
            assertThat(clientA).isNotSameAs(clientB);
            assertThat(RocketMQAckClient.getReferenceCount(a)).isEqualTo(1);
            assertThat(RocketMQAckClient.getReferenceCount(b)).isEqualTo(1);
        } finally {
            RocketMQAckClient.release(a);
            RocketMQAckClient.release(b);
        }

        assertThat(RocketMQAckClient.getReferenceCount(a)).isZero();
        assertThat(RocketMQAckClient.getReferenceCount(b)).isZero();
    }

    @Test
    void releaseWithoutAcquireIsANoOp() {
        final Configuration configuration = config("no-acquire:8080");
        RocketMQAckClient.release(configuration);
        assertThat(RocketMQAckClient.getReferenceCount(configuration)).isZero();
    }
}
