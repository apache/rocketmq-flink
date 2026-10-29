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

package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A route fetch failure must never be interpreted as "those queues are gone": during a nameserver
 * outage the topic route is unchanged, and revoking the whole assignment would cause a mass
 * rebalance plus an offset rewind once the nameserver is back.
 */
public class RocketMQSourceEnumeratorDiscoveryToleranceTest {

    private static final String BROKER = "broker-a";

    @Test
    public void allTopicsFetchFailedSkipsDiscoveryRoundTest() throws Exception {
        InnerConsumer consumer = mock(InnerConsumer.class);
        when(consumer.fetchMessageQueues("t1"))
                .thenReturn(failedFuture(new RuntimeException("nameserver unreachable")));
        when(consumer.fetchMessageQueues("t2"))
                .thenReturn(failedFuture(new RuntimeException("nameserver unreachable")));
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer, "t1;t2");

        assertThat(invokeRequestServiceDiscovery(enumerator)).isNull();
    }

    @Test
    public void partialFetchFailureKeepsFailedTopicQueuesTest() throws Exception {
        InnerConsumer consumer = mock(InnerConsumer.class);
        when(consumer.fetchMessageQueues("t1"))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Arrays.asList(queue("t1", 0), queue("t1", 1))));
        when(consumer.fetchMessageQueues("t2"))
                .thenReturn(failedFuture(new RuntimeException("nameserver unreachable")));
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer, "t1;t2");

        Set<MessageQueue> discovered = invokeRequestServiceDiscovery(enumerator);

        assertThat(discovered).containsExactlyInAnyOrder(queue("t1", 0), queue("t1", 1));
    }

    @Test
    public void successfulDiscoveryReturnsAllQueuesTest() throws Exception {
        InnerConsumer consumer = mock(InnerConsumer.class);
        when(consumer.fetchMessageQueues("t1"))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Arrays.asList(queue("t1", 0), queue("t1", 1))));
        when(consumer.fetchMessageQueues("t2"))
                .thenReturn(CompletableFuture.completedFuture(Arrays.asList(queue("t2", 0))));
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer, "t1;t2");

        Set<MessageQueue> discovered = invokeRequestServiceDiscovery(enumerator);

        assertThat(discovered)
                .containsExactlyInAnyOrder(queue("t1", 0), queue("t1", 1), queue("t2", 0));
    }

    @Test
    public void emptyDiscoveryWithAllocatedQueuesKeepsAssignmentTest() throws Exception {
        InnerConsumer consumer = mock(InnerConsumer.class);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer, "t1");
        setAllocatedSet(enumerator, new HashSet<>(Arrays.asList(queue("t1", 0), queue("t1", 1))));

        Method handle =
                RocketMQSourceEnumerator.class.getDeclaredMethod(
                        "handleSourceQueueChange", Set.class, Throwable.class);
        handle.setAccessible(true);
        // Must return without touching the split assignment or scheduling any re-initialization.
        handle.invoke(enumerator, new HashSet<MessageQueue>(), null);

        assertThat(allocatedSetOf(enumerator)).hasSize(2);
    }

    @Test
    public void emptyLatestSetAloneWouldRevokeEverythingTest() throws Exception {
        // Documents the hazard the guards above prevent: diffing an empty route against the
        // allocated set reports every queue as decreased.
        InnerConsumer consumer = mock(InnerConsumer.class);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer, "t1");
        setAllocatedSet(enumerator, new HashSet<>(Arrays.asList(queue("t1", 0), queue("t1", 1))));

        Method diff =
                RocketMQSourceEnumerator.class.getDeclaredMethod(
                        "getSourceChangeResult", Set.class);
        diff.setAccessible(true);
        Object changeResult = diff.invoke(enumerator, new HashSet<MessageQueue>());

        Field decreaseField = changeResult.getClass().getDeclaredField("decreaseSet");
        decreaseField.setAccessible(true);
        assertThat((Collection<?>) decreaseField.get(changeResult)).hasSize(2);
    }

    private static RocketMQSourceEnumerator newEnumerator(InnerConsumer consumer, String topics)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(RocketMQSourceOptions.TOPIC, topics);
        RocketMQSourceEnumerator enumerator =
                new RocketMQSourceEnumerator(
                        OffsetsSelector.earliest(),
                        OffsetsSelector.latest(),
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        configuration,
                        null);
        Field consumerField = RocketMQSourceEnumerator.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(enumerator, consumer);
        return enumerator;
    }

    @SuppressWarnings("unchecked")
    private static Set<MessageQueue> invokeRequestServiceDiscovery(RocketMQSourceEnumerator e)
            throws Exception {
        Method method = RocketMQSourceEnumerator.class.getDeclaredMethod("requestServiceDiscovery");
        method.setAccessible(true);
        return (Set<MessageQueue>) method.invoke(e);
    }

    @SuppressWarnings("unchecked")
    private static void setAllocatedSet(RocketMQSourceEnumerator e, Set<MessageQueue> queues)
            throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("allocatedSet");
        field.setAccessible(true);
        ((java.util.Map<MessageQueue, Byte>) field.get(e))
                .putAll(queues.stream().collect(Collectors.toMap(q -> q, q -> (byte) 0)));
    }

    @SuppressWarnings("unchecked")
    private static Set<MessageQueue> allocatedSetOf(RocketMQSourceEnumerator e) throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("allocatedSet");
        field.setAccessible(true);
        return ((java.util.Map<MessageQueue, Byte>) field.get(e)).keySet();
    }

    private static MessageQueue queue(String topic, int queueId) {
        return new MessageQueue(topic, BROKER, queueId);
    }

    private static CompletableFuture<Collection<MessageQueue>> failedFuture(Throwable t) {
        CompletableFuture<Collection<MessageQueue>> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }
}
