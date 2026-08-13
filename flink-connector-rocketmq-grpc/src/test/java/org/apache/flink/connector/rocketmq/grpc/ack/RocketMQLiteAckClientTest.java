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

/** Tests for the reference counting behaviour of {@link RocketMQLiteAckClient#acquire}. */
class RocketMQLiteAckClientTest {

    private static Configuration config(String endpoints) {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ENDPOINTS, endpoints);
        return configuration;
    }

    @Test
    void sameConfigurationSharesASingleReferenceCountedClient() {
        final Configuration configuration = config("127.0.0.1:8080");
        assertThat(RocketMQLiteAckClient.getReferenceCount(configuration)).isZero();

        final RocketMQLiteAckClient first = RocketMQLiteAckClient.acquire(configuration);
        final RocketMQLiteAckClient second = RocketMQLiteAckClient.acquire(configuration);

        // The same client is shared and the reference count is incremented per acquire.
        assertThat(second).isSameAs(first);
        assertThat(RocketMQLiteAckClient.getReferenceCount(configuration)).isEqualTo(2);

        RocketMQLiteAckClient.release(configuration);
        assertThat(RocketMQLiteAckClient.getReferenceCount(configuration)).isEqualTo(1);

        RocketMQLiteAckClient.release(configuration);
        assertThat(RocketMQLiteAckClient.getReferenceCount(configuration)).isZero();
    }

    @Test
    void differentConfigurationsGetDistinctClients() {
        final Configuration a = config("host-a:8080");
        final Configuration b = config("host-b:8080");

        final RocketMQLiteAckClient clientA = RocketMQLiteAckClient.acquire(a);
        final RocketMQLiteAckClient clientB = RocketMQLiteAckClient.acquire(b);
        try {
            assertThat(clientA).isNotSameAs(clientB);
            assertThat(RocketMQLiteAckClient.getReferenceCount(a)).isEqualTo(1);
            assertThat(RocketMQLiteAckClient.getReferenceCount(b)).isEqualTo(1);
        } finally {
            RocketMQLiteAckClient.release(a);
            RocketMQLiteAckClient.release(b);
        }

        assertThat(RocketMQLiteAckClient.getReferenceCount(a)).isZero();
        assertThat(RocketMQLiteAckClient.getReferenceCount(b)).isZero();
    }

    @Test
    void releaseWithoutAcquireIsANoOp() {
        final Configuration configuration = config("no-acquire:8080");
        RocketMQLiteAckClient.release(configuration);
        assertThat(RocketMQLiteAckClient.getReferenceCount(configuration)).isZero();
    }
}
