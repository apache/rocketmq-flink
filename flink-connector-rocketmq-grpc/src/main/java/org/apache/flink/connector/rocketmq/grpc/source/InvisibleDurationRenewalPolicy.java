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

package org.apache.flink.connector.rocketmq.grpc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;

/**
 * A user-provided policy that decides whether a received message that is still buffered inside the
 * source (i.e. not yet emitted downstream) should have its invisible duration renewed.
 *
 * <p>The source invokes {@link #renew(MessageView, int)} {@link
 * RocketMQGrpcSourceOptions#RENEWAL_AHEAD_TIME} ahead of the moment the message would become
 * visible again (for example with a 60s invisible duration and a 5s ahead time, the policy is
 * consulted 55s after the message was received). Returning a positive duration triggers a {@code
 * changeInvisibleDuration} call with that duration and schedules the next consultation; returning
 * {@code null} (or a non-positive duration) stops renewing so the message becomes visible again
 * after the current invisible duration elapses.
 *
 * <p>Messages that have already been handed to the record emitter are never renewed, because
 * renewing refreshes the receipt handle and would invalidate the handle travelling downstream.
 *
 * <p>Implementations are instantiated reflectively from {@link
 * RocketMQGrpcSourceOptions#RENEWAL_POLICY_CLASS} and therefore need a public no-argument
 * constructor. One instance is created per split reader; invocations happen on a single renewal
 * thread.
 */
@PublicEvolving
public interface InvisibleDurationRenewalPolicy extends Serializable {

    /**
     * Configure this policy with the source configuration. Called once directly after
     * instantiation.
     */
    default void configure(Configuration configuration) {}

    /**
     * Decide whether to renew the invisible duration of the given message.
     *
     * @param messageView the message still buffered inside the source.
     * @param renewalCount how many times this message has already been renewed; {@code 0} on the
     *     first consultation.
     * @return the new invisible duration, or {@code null} to stop renewing.
     */
    @Nullable
    Duration renew(MessageView messageView, int renewalCount);
}
