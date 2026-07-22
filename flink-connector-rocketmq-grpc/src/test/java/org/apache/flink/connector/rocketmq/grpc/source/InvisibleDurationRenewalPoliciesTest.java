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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link InvisibleDurationRenewalPolicies}. */
class InvisibleDurationRenewalPoliciesTest {

    @Test
    void returnsNullWhenNoPolicyConfiguredTest() {
        assertThat(InvisibleDurationRenewalPolicies.createFromConfiguration(new Configuration()))
                .isNull();
    }

    @Test
    void instantiatesAndConfiguresPolicyTest() {
        final Configuration configuration = new Configuration();
        configuration.set(
                RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS,
                RecordingRenewalPolicy.class.getName());
        final InvisibleDurationRenewalPolicy policy =
                InvisibleDurationRenewalPolicies.createFromConfiguration(configuration);
        assertThat(policy).isInstanceOf(RecordingRenewalPolicy.class);
        assertThat(((RecordingRenewalPolicy) policy).configured).isTrue();
    }

    @Test
    void failsForUnknownClassTest() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS, "does.not.Exist");
        assertThatThrownBy(
                        () ->
                                InvisibleDurationRenewalPolicies.createFromConfiguration(
                                        configuration))
                .isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void failsForNonPolicyClassTest() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS, String.class.getName());
        assertThatThrownBy(
                        () ->
                                InvisibleDurationRenewalPolicies.createFromConfiguration(
                                        configuration))
                .isInstanceOf(FlinkRuntimeException.class);
    }

    /** A renewal policy that records whether it was configured. */
    public static class RecordingRenewalPolicy implements InvisibleDurationRenewalPolicy {

        private static final long serialVersionUID = 1L;

        boolean configured;

        @Override
        public void configure(Configuration configuration) {
            this.configured = true;
        }

        @Nullable
        @Override
        public Duration renew(MessageView messageView, int renewalCount) {
            return null;
        }
    }
}
