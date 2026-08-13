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

package org.apache.flink.connector.rocketmq.grpc.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;

import org.apache.rocketmq.client.apis.SessionCredentialsProvider;

import javax.annotation.Nullable;

/**
 * Resolves RocketMQ session credentials for a given proxy endpoint on the TaskManager, so that
 * plaintext access/secret keys never need to be placed in the job graph or the Flink {@link
 * Configuration}.
 *
 * <p>Implementations are instantiated reflectively from the class name configured under {@code
 * rocketmq.client.credentials-resolver-class} and therefore must be public and provide a public
 * no-argument constructor. The instance is created locally on each TaskManager (never serialized),
 * which makes it safe to read credentials from environment variables, mounted secret files or an
 * external KMS.
 *
 * <p>Because the resolver is keyed by endpoint, a single downstream acknowledgement operator can
 * serve receipt handles originating from multiple sources that point at different clusters with
 * different credentials.
 *
 * <p>When a resolver class is configured, it takes precedence over the static {@code
 * rocketmq.client.access-key} / {@code rocketmq.client.secret-key} options.
 */
@PublicEvolving
public interface CredentialsResolver {

    /**
     * Called once right after instantiation with the operator configuration, before any {@link
     * #resolve(String)} call. Implementations may read custom options from it.
     *
     * @param configuration the operator configuration.
     */
    default void configure(Configuration configuration) {}

    /**
     * Resolve the session credentials for the given proxy endpoint.
     *
     * @param endpoint the proxy endpoint the SDK client will connect to, e.g. {@code
     *     "127.0.0.1:8081"}.x
     * @return the credentials provider for the endpoint, or {@code null} if the endpoint requires
     *     no authentication.
     */
    @Nullable
    SessionCredentialsProvider resolve(String endpoint);
}
