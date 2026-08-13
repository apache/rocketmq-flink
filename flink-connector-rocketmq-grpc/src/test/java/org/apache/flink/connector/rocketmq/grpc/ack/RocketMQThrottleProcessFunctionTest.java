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
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the throttle branching and safeguards of {@link RocketMQThrottleProcessFunction}. */
class RocketMQThrottleProcessFunctionTest {

    private static RocketMQReceiptHandle handle(String messageId, int deliveryAttempt) {
        return new RocketMQReceiptHandle(
                "127.0.0.1:8080",
                "ns",
                "grp",
                "topic",
                "sub",
                messageId,
                "rh-" + messageId,
                deliveryAttempt);
    }

    @Test
    void emptyPolicyResultAcknowledges() throws Exception {
        final RecordingThrottleFunction fn =
                new RecordingThrottleFunction(
                        value -> Optional.empty(), 16, Duration.ofMinutes(30));
        final ListCollector out = new ListCollector();

        fn.processElement(new AckableMessage<>("v", handle("m1", 0)), null, out);

        assertThat(fn.acked).containsExactly("m1");
        assertThat(fn.changed).isEmpty();
        assertThat(out.values).containsExactly("v");
    }

    @Test
    void policyDelayChangesInvisibleDuration() throws Exception {
        final Duration delay = Duration.ofSeconds(10);
        final RecordingThrottleFunction fn =
                new RecordingThrottleFunction(
                        value -> Optional.of(delay), 16, Duration.ofMinutes(30));
        final ListCollector out = new ListCollector();

        fn.processElement(new AckableMessage<>("v", handle("m1", 0)), null, out);

        assertThat(fn.changed).containsEntry("m1", delay);
        assertThat(fn.acked).isEmpty();
        // The value is always forwarded downstream.
        assertThat(out.values).containsExactly("v");
    }

    @Test
    void requestedDelayIsCappedToMaxInvisibleDuration() throws Exception {
        final Duration cap = Duration.ofSeconds(30);
        final RecordingThrottleFunction fn =
                new RecordingThrottleFunction(value -> Optional.of(Duration.ofHours(1)), 16, cap);
        final ListCollector out = new ListCollector();

        fn.processElement(new AckableMessage<>("v", handle("m1", 0)), null, out);

        assertThat(fn.changed).containsEntry("m1", cap);
    }

    @Test
    void deferSafeguardForcesAckOnceMaxDeliveryAttemptReached() throws Exception {
        final int maxAttempt = 3;
        final RecordingThrottleFunction fn =
                new RecordingThrottleFunction(
                        value -> Optional.of(Duration.ofSeconds(10)),
                        maxAttempt,
                        Duration.ofMinutes(30));
        final ListCollector out = new ListCollector();

        // Below the bound: still deferred.
        fn.processElement(new AckableMessage<>("v", handle("m1", maxAttempt - 1)), null, out);
        assertThat(fn.changed).containsKey("m1");
        assertThat(fn.acked).isEmpty();

        // At the bound: the safeguard trips and the message is acknowledged instead.
        fn.processElement(new AckableMessage<>("v", handle("m2", maxAttempt)), null, out);
        assertThat(fn.acked).containsExactly("m2");
    }

    private static final class RecordingThrottleFunction
            extends RocketMQThrottleProcessFunction<String> {

        private static final long serialVersionUID = 1L;

        private final List<String> acked = new ArrayList<>();
        private final java.util.Map<String, Duration> changed = new java.util.LinkedHashMap<>();

        RecordingThrottleFunction(
                MessageThrottlePolicy<String> policy,
                int maxDeliveryAttempt,
                Duration maxInvisible) {
            super(new Configuration(), policy, maxDeliveryAttempt, maxInvisible);
        }

        @Override
        protected void ack(RocketMQReceiptHandle handle) {
            acked.add(handle.getMessageId());
        }

        @Override
        protected void changeInvisibleDuration(
                RocketMQReceiptHandle handle, Duration invisibleDuration) {
            changed.put(handle.getMessageId(), invisibleDuration);
        }
    }

    private static final class ListCollector implements Collector<String> {

        private final List<String> values = new ArrayList<>();

        @Override
        public void collect(String record) {
            values.add(record);
        }

        @Override
        public void close() {}
    }
}
