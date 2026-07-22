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

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/**
 * A per-message throttling policy used by {@link RocketMQThrottleProcessFunction}. For each value
 * it decides whether the message should be acknowledged normally or deferred (throttled) by
 * extending its invisible duration.
 *
 * @param <T> the value type carried by the {@code AckableMessage}.
 */
@PublicEvolving
@FunctionalInterface
public interface MessageThrottlePolicy<T> extends Serializable {

    /**
     * Decide how to handle the given value.
     *
     * @param value the deserialized message value.
     * @return an empty {@link Optional} to acknowledge the message normally, or a positive {@link
     *     Duration} to defer its re-delivery by that amount (throttle).
     */
    Optional<Duration> onMessage(T value);
}
