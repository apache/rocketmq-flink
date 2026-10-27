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
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for verifying that configuration parameters are applied to the RocketMQ producer. */
class InnerProducerImplTest {

    private TransactionMQProducer getProducerViaReflection(InnerProducerImpl impl)
            throws Exception {
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

    /**
     * The node that accepted the half message is the only one holding its transaction context, so
     * the commit must go to the address the writer recorded. Resolving it from the committer's own
     * route table can name another node, which discards the commit and still answers with a success
     * code.
     */
    @Test
    void commitGoesToTheAddressRecordedByTheWriterTest() throws Exception {
        MQClientInstance clientInstance = Mockito.mock(MQClientInstance.class);
        NettyRemotingClient remotingClient = stubRemoting(clientInstance);

        try (InnerProducerImpl impl = new InnerProducerImpl(configuration())) {
            setClientInstance(impl, clientInstance);

            impl.endTransaction(committable("10.0.38.29:8080"), TransactionResult.COMMIT);

            Mockito.verify(remotingClient)
                    .invokeSync(
                            Mockito.eq("10.0.38.29:8080"),
                            Mockito.any(RemotingCommand.class),
                            Mockito.anyLong());
            Mockito.verify(clientInstance, Mockito.never())
                    .findBrokerAddressInPublish(Mockito.anyString());
        }
    }

    /** Committables restored from state written before the address was recorded have no address. */
    @Test
    void commitFallsBackToThePublishRouteWithoutRecordedAddressTest() throws Exception {
        MQClientInstance clientInstance = Mockito.mock(MQClientInstance.class);
        Mockito.when(clientInstance.getBrokerNameFromMessageQueue(Mockito.any()))
                .thenReturn("broker-a");
        Mockito.when(clientInstance.findBrokerAddressInPublish("broker-a"))
                .thenReturn("10.0.26.60:8080");
        NettyRemotingClient remotingClient = stubRemoting(clientInstance);

        try (InnerProducerImpl impl = new InnerProducerImpl(configuration())) {
            setClientInstance(impl, clientInstance);

            impl.endTransaction(committable(null), TransactionResult.COMMIT);

            Mockito.verify(remotingClient)
                    .invokeSync(
                            Mockito.eq("10.0.26.60:8080"),
                            Mockito.any(RemotingCommand.class),
                            Mockito.anyLong());
        }
    }

    /**
     * A checkpoint can carry thousands of committables; committing them must not depend on a
     * bounded executor queue that rejects the surplus (a rejected commit is a lost commit once the
     * server-side transaction context expires).
     */
    @Test
    void commitBurstShouldNotRejectTest() throws Exception {
        MQClientInstance clientInstance = Mockito.mock(MQClientInstance.class);
        NettyRemotingClient remotingClient = stubRemoting(clientInstance);

        try (InnerProducerImpl impl = new InnerProducerImpl(configuration())) {
            setClientInstance(impl, clientInstance);

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                futures.add(impl.commit(committable("10.0.26.60:8080")));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.SECONDS);
            Mockito.verify(remotingClient, Mockito.times(3000))
                    .invokeSync(
                            Mockito.eq("10.0.26.60:8080"),
                            Mockito.any(RemotingCommand.class),
                            Mockito.anyLong());
        }
    }

    @Test
    void commitFailureShouldCompleteTheFutureExceptionallyTest() throws Exception {
        MQClientInstance clientInstance = Mockito.mock(MQClientInstance.class);
        MQClientAPIImpl api = Mockito.mock(MQClientAPIImpl.class);
        NettyRemotingClient remotingClient = Mockito.mock(NettyRemotingClient.class);
        Mockito.when(clientInstance.getMQClientAPIImpl()).thenReturn(api);
        Mockito.when(api.getRemotingClient()).thenReturn(remotingClient);
        Mockito.when(
                        remotingClient.invokeSync(
                                Mockito.anyString(),
                                Mockito.any(RemotingCommand.class),
                                Mockito.anyLong()))
                .thenThrow(new RemotingSendRequestException("10.0.26.60:8080"));

        try (InnerProducerImpl impl = new InnerProducerImpl(configuration())) {
            setClientInstance(impl, clientInstance);

            CompletableFuture<Void> future = impl.commit(committable("10.0.26.60:8080"));
            assertThat(future).isCompletedExceptionally();
        }
    }

    private static Configuration configuration() {
        Configuration config = new Configuration();
        config.setString(RocketMQSinkOptions.ENDPOINTS, "127.0.0.1:9876");
        config.setString(RocketMQSinkOptions.PRODUCER_GROUP, "test-group");
        return config;
    }

    private static NettyRemotingClient stubRemoting(MQClientInstance clientInstance)
            throws Exception {
        MQClientAPIImpl api = Mockito.mock(MQClientAPIImpl.class);
        NettyRemotingClient remotingClient = Mockito.mock(NettyRemotingClient.class);
        Mockito.when(clientInstance.getMQClientAPIImpl()).thenReturn(api);
        Mockito.when(api.getRemotingClient()).thenReturn(remotingClient);
        Mockito.when(
                        remotingClient.invokeSync(
                                Mockito.anyString(),
                                Mockito.any(RemotingCommand.class),
                                Mockito.anyLong()))
                .thenReturn(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""));
        return remotingClient;
    }

    private static void setClientInstance(InnerProducerImpl impl, MQClientInstance instance)
            throws Exception {
        Field field = InnerProducerImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(impl, instance);
    }

    private static SendCommittable committable(String brokerAddress) {
        SendCommittable committable = new SendCommittable();
        committable.setTopic("TopicTest");
        committable.setBrokerName("broker-a");
        committable.setQueueId(3);
        committable.setQueueOffset(42L);
        committable.setMsgId("msg-1");
        // 32 hex chars -- 8 ip, 8 port, 16 commit log offset -- as decoded by MessageDecoder.
        committable.setOffsetMsgId("0A00000100002382000000000000002A");
        committable.setTransactionId("trans-1");
        committable.setBrokerAddress(brokerAddress);
        return committable;
    }
}
