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
import org.apache.flink.connector.rocketmq.common.lock.SpinLock;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A queue that was revoked and later reappears (broker rolling restart, scale-down then scale-up)
 * must resume from its last checkpointed offset, the same rule the migration path applies to
 * surviving queues. Re-resolving with the configured starting selector (here EARLIEST) would replay
 * the whole queue; only genuinely new queues may go through the selector.
 */
public class RocketMQSourceEnumeratorRevokedOffsetResumeTest {

    private static final String BROKER = "broker-a";
    private static final String TOPIC = "t1";

    @Test
    public void revokedQueueReaddedResumesFromCheckpointOffsetTest() throws Exception {
        InnerConsumer consumer = consumerWithOffsets(0L, 100L);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer);
        MessageQueue q0 = queue(0);
        setAllocatedSet(enumerator, new HashSet<>(Arrays.asList(q0)));
        checkedOffsetsOf(enumerator).put(q0, 42L);

        Object decreaseResult =
                invokeInitializeSourceSplits(
                        enumerator, new HashSet<>(), new HashSet<>(Arrays.asList(q0)));
        Set<RocketMQSourceSplit> decreaseSplits = splitsOf(decreaseResult, "decreaseSet");
        assertThat(decreaseSplits).hasSize(1);
        assertThat(decreaseSplits.iterator().next().getIsIncrease()).isFalse();
        assertThat(allocatedSetOf(enumerator)).isEmpty();
        assertThat(revokedQueueOffsetsOf(enumerator)).containsEntry(q0, 42L);

        Object increaseResult =
                invokeInitializeSourceSplits(
                        enumerator, new HashSet<>(Arrays.asList(q0)), new HashSet<>());
        RocketMQSourceSplit readded = splitFor(splitsOf(increaseResult, "increaseSet"), q0);
        assertThat(readded.getIsIncrease()).isTrue();
        assertThat(readded.getStartingOffset()).isEqualTo(42L);
        assertThat(checkedOffsetsOf(enumerator)).containsEntry(q0, 42L);
        assertThat(revokedQueueOffsetsOf(enumerator)).isEmpty();
    }

    @Test
    public void genuinelyNewQueueUsesConfiguredSelectorTest() throws Exception {
        InnerConsumer consumer = consumerWithOffsets(0L, 100L);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer);
        MessageQueue q1 = queue(1);

        Object increaseResult =
                invokeInitializeSourceSplits(
                        enumerator, new HashSet<>(Arrays.asList(q1)), new HashSet<>());

        // Never allocated before: the EARLIEST selector resolves the min offset (0).
        assertThat(splitFor(splitsOf(increaseResult, "increaseSet"), q1).getStartingOffset())
                .isEqualTo(0L);
    }

    @Test
    public void mixedReaddedAndNewQueuesResolveIndependentlyTest() throws Exception {
        InnerConsumer consumer = consumerWithOffsets(0L, 100L);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer);
        MessageQueue q0 = queue(0);
        MessageQueue q1 = queue(1);
        setAllocatedSet(enumerator, new HashSet<>(Arrays.asList(q0)));
        checkedOffsetsOf(enumerator).put(q0, 42L);
        invokeInitializeSourceSplits(enumerator, new HashSet<>(), new HashSet<>(Arrays.asList(q0)));

        Object increaseResult =
                invokeInitializeSourceSplits(
                        enumerator, new HashSet<>(Arrays.asList(q0, q1)), new HashSet<>());

        Set<RocketMQSourceSplit> increaseSplits = splitsOf(increaseResult, "increaseSet");
        assertThat(splitFor(increaseSplits, q0).getStartingOffset()).isEqualTo(42L);
        assertThat(splitFor(increaseSplits, q1).getStartingOffset()).isEqualTo(0L);
    }

    @Test
    public void nonCheckpointedOffsetFallsBackToSelectorTest() throws Exception {
        InnerConsumer consumer = consumerWithOffsets(0L, 100L);
        RocketMQSourceEnumerator enumerator = newEnumerator(consumer);
        MessageQueue q0 = queue(0);
        setAllocatedSet(enumerator, new HashSet<>(Arrays.asList(q0)));
        // NO_STOPPING_OFFSET: e.g. restored from a checkpoint before the first offset report.
        // There is no meaningful checkpointed progress to retain.
        checkedOffsetsOf(enumerator).put(q0, RocketMQSourceSplit.NO_STOPPING_OFFSET);

        invokeInitializeSourceSplits(enumerator, new HashSet<>(), new HashSet<>(Arrays.asList(q0)));
        assertThat(revokedQueueOffsetsOf(enumerator)).isEmpty();

        Object increaseResult =
                invokeInitializeSourceSplits(
                        enumerator, new HashSet<>(Arrays.asList(q0)), new HashSet<>());
        assertThat(splitFor(splitsOf(increaseResult, "increaseSet"), q0).getStartingOffset())
                .isEqualTo(0L);
    }

    private static RocketMQSourceEnumerator newEnumerator(InnerConsumer consumer) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RocketMQSourceOptions.TOPIC, TOPIC);
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

    private static InnerConsumer consumerWithOffsets(long minOffset, long maxOffset) {
        InnerConsumer consumer = mock(InnerConsumer.class);
        when(consumer.seekMinOffset(any(MessageQueue.class)))
                .thenReturn(CompletableFuture.completedFuture(minOffset));
        when(consumer.seekMaxOffset(any(MessageQueue.class)))
                .thenReturn(CompletableFuture.completedFuture(maxOffset));
        return consumer;
    }

    /**
     * Invokes the private initialization step directly. The production path releases the enumerator
     * lock in {@code handleSplitChanges}; bypassing that handler means the test must release it
     * here, otherwise the next round spins forever on the non-reentrant lock.
     */
    private static Object invokeInitializeSourceSplits(
            RocketMQSourceEnumerator e, Set<MessageQueue> increase, Set<MessageQueue> decrease)
            throws Exception {
        Class<?> changeClass =
                Class.forName(
                        "org.apache.flink.connector.rocketmq.source.enumerator.RocketMQSourceEnumerator$SourceChangeResult");
        Constructor<?> constructor = changeClass.getDeclaredConstructor(Set.class, Set.class);
        constructor.setAccessible(true);
        Object changeResult = constructor.newInstance(increase, decrease);
        Method method =
                RocketMQSourceEnumerator.class.getDeclaredMethod(
                        "initializeSourceSplits", changeClass);
        method.setAccessible(true);
        try {
            return method.invoke(e, changeResult);
        } finally {
            Field lockField = RocketMQSourceEnumerator.class.getDeclaredField("lock");
            lockField.setAccessible(true);
            ((SpinLock) lockField.get(e)).unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<RocketMQSourceSplit> splitsOf(Object splitChangeResult, String fieldName)
            throws Exception {
        Field field = splitChangeResult.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Set<RocketMQSourceSplit>) field.get(splitChangeResult);
    }

    private static RocketMQSourceSplit splitFor(
            Set<RocketMQSourceSplit> splits, MessageQueue messageQueue) {
        return splits.stream()
                .filter(split -> split.getMessageQueue().equals(messageQueue))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No split for " + messageQueue));
    }

    @SuppressWarnings("unchecked")
    private static void setAllocatedSet(RocketMQSourceEnumerator e, Set<MessageQueue> queues)
            throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("allocatedSet");
        field.setAccessible(true);
        Map<MessageQueue, Byte> allocatedSet = (Map<MessageQueue, Byte>) field.get(e);
        queues.forEach(q -> allocatedSet.put(q, (byte) 0));
    }

    @SuppressWarnings("unchecked")
    private static Set<MessageQueue> allocatedSetOf(RocketMQSourceEnumerator e) throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("allocatedSet");
        field.setAccessible(true);
        return ((Map<MessageQueue, Byte>) field.get(e)).keySet();
    }

    @SuppressWarnings("unchecked")
    private static Map<MessageQueue, Long> checkedOffsetsOf(RocketMQSourceEnumerator e)
            throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("checkedOffsets");
        field.setAccessible(true);
        return (Map<MessageQueue, Long>) field.get(e);
    }

    @SuppressWarnings("unchecked")
    private static Map<MessageQueue, Long> revokedQueueOffsetsOf(RocketMQSourceEnumerator e)
            throws Exception {
        Field field = RocketMQSourceEnumerator.class.getDeclaredField("revokedQueueOffsets");
        field.setAccessible(true);
        return (Map<MessageQueue, Long>) field.get(e);
    }

    private static MessageQueue queue(int queueId) {
        return new MessageQueue(TOPIC, BROKER, queueId);
    }
}
