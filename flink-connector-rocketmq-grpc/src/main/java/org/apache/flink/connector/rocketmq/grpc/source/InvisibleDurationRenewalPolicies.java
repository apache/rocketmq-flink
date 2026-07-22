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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

/** Instantiates {@link InvisibleDurationRenewalPolicy} implementations reflectively. */
@Internal
public final class InvisibleDurationRenewalPolicies {

    private InvisibleDurationRenewalPolicies() {}

    /**
     * Create the {@link InvisibleDurationRenewalPolicy} configured under {@link
     * RocketMQGrpcSourceOptions#RENEWAL_POLICY_CLASS}, or return {@code null} if none is
     * configured. The policy is instantiated reflectively and {@link
     * InvisibleDurationRenewalPolicy#configure(Configuration) configured} before being returned.
     */
    @Nullable
    public static InvisibleDurationRenewalPolicy createFromConfiguration(
            Configuration configuration) {
        final String className = configuration.get(RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS);
        if (StringUtils.isNullOrWhitespaceOnly(className)) {
            return null;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = InvisibleDurationRenewalPolicies.class.getClassLoader();
        }
        try {
            final InvisibleDurationRenewalPolicy policy =
                    Class.forName(className, true, classLoader)
                            .asSubclass(InvisibleDurationRenewalPolicy.class)
                            .getDeclaredConstructor()
                            .newInstance();
            policy.configure(configuration);
            return policy;
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new FlinkRuntimeException(
                    "Failed to instantiate the invisible duration renewal policy '"
                            + className
                            + "'. It must be a public implementation of "
                            + InvisibleDurationRenewalPolicy.class.getName()
                            + " with a public no-argument constructor.",
                    e);
        }
    }
}
