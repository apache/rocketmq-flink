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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * A convenience one-stop throttle operator. For every {@link AckableMessage} it consults a {@link
 * MessageThrottlePolicy}: when the policy returns a delay it {@linkplain
 * RocketMQAckProcessFunction#changeInvisibleDuration changes the invisible duration} (deferring the
 * message to throttle its (sub) topic); otherwise it {@linkplain RocketMQAckProcessFunction#ack
 * acknowledges} the message. The underlying value is always forwarded downstream unchanged.
 *
 * <p>Two safeguards prevent a persistently throttled message from being deferred forever and
 * eventually pushed into the dead-letter queue once its delivery attempts are exhausted:
 *
 * <ul>
 *   <li>{@code maxDeliveryAttempt} — once the message's delivery attempt reaches this bound the
 *       message is acknowledged instead of deferred again.
 *   <li>{@code maxInvisibleDuration} — every requested delay is capped to this value.
 * </ul>
 *
 * @param <T> the value type carried by the {@code AckableMessage}.
 */
@PublicEvolving
public class RocketMQThrottleProcessFunction<T>
        extends RocketMQAckProcessFunction<AckableMessage<T>, T> {

    private static final long serialVersionUID = 1L;

    /** Default cap on the number of times a message may be deferred before it is acknowledged. */
    public static final int DEFAULT_MAX_DELIVERY_ATTEMPT = 16;

    /** Default cap on a single requested invisible duration. */
    public static final Duration DEFAULT_MAX_INVISIBLE_DURATION = Duration.ofMinutes(30);

    private final MessageThrottlePolicy<T> policy;
    private final int maxDeliveryAttempt;
    private final Duration maxInvisibleDuration;

    public RocketMQThrottleProcessFunction(
            Configuration configuration, MessageThrottlePolicy<T> policy) {
        this(configuration, policy, DEFAULT_MAX_DELIVERY_ATTEMPT, DEFAULT_MAX_INVISIBLE_DURATION);
    }

    public RocketMQThrottleProcessFunction(
            Configuration configuration,
            MessageThrottlePolicy<T> policy,
            int maxDeliveryAttempt,
            Duration maxInvisibleDuration) {
        super(configuration);
        this.policy = Objects.requireNonNull(policy, "policy should not be null");
        this.maxDeliveryAttempt = maxDeliveryAttempt;
        this.maxInvisibleDuration =
                Objects.requireNonNull(
                        maxInvisibleDuration, "maxInvisibleDuration should not be null");
    }

    @Override
    public void processElement(
            AckableMessage<T> message,
            ProcessFunction<AckableMessage<T>, T>.Context context,
            Collector<T> out) {
        final RocketMQReceiptHandle handle = message.getHandle();
        final Optional<Duration> delay = policy.onMessage(message.getValue());

        if (delay.isPresent() && handle.getDeliveryAttempt() < maxDeliveryAttempt) {
            changeInvisibleDuration(handle, capDelay(delay.get()));
        } else {
            // Either the policy accepted the message, or the defer safeguard tripped: ack it so it
            // is not eventually pushed into the dead-letter queue.
            ack(handle);
        }

        out.collect(message.getValue());
    }

    private Duration capDelay(Duration requested) {
        return requested.compareTo(maxInvisibleDuration) > 0 ? maxInvisibleDuration : requested;
    }
}
