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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;

import javax.annotation.Nullable;

/** Instantiates {@link CredentialsResolver}s and applies credentials to SDK client builders. */
@Internal
public final class CredentialsResolvers {

    private CredentialsResolvers() {}

    /**
     * Create the {@link CredentialsResolver} configured under {@link
     * RocketMQGrpcOptions#CREDENTIALS_RESOLVER_CLASS}, or return {@code null} if none is
     * configured. The resolver is instantiated reflectively and {@link
     * CredentialsResolver#configure(Configuration) configured} before being returned.
     */
    @Nullable
    public static CredentialsResolver createFromConfiguration(Configuration configuration) {
        final String className = configuration.get(RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS);
        if (StringUtils.isNullOrWhitespaceOnly(className)) {
            return null;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = CredentialsResolvers.class.getClassLoader();
        }
        try {
            final CredentialsResolver resolver =
                    Class.forName(className, true, classLoader)
                            .asSubclass(CredentialsResolver.class)
                            .getDeclaredConstructor()
                            .newInstance();
            resolver.configure(configuration);
            return resolver;
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new FlinkRuntimeException(
                    "Failed to instantiate the credentials resolver '"
                            + className
                            + "'. It must be a public implementation of "
                            + CredentialsResolver.class.getName()
                            + " with a public no-argument constructor.",
                    e);
        }
    }

    /**
     * Apply credentials for the given endpoint to the SDK client builder. A configured {@link
     * CredentialsResolver} takes precedence; otherwise the static access/secret key options are
     * used when both are present.
     */
    public static void applyCredentials(
            ClientConfigurationBuilder builder,
            String endpoint,
            Configuration configuration,
            @Nullable CredentialsResolver resolver) {
        if (resolver != null) {
            final SessionCredentialsProvider provider = resolver.resolve(endpoint);
            if (provider != null) {
                builder.setCredentialProvider(provider);
            }
            return;
        }
        final String accessKey = configuration.get(RocketMQGrpcOptions.ACCESS_KEY);
        final String secretKey = configuration.get(RocketMQGrpcOptions.SECRET_KEY);
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            builder.setCredentialProvider(
                    new StaticSessionCredentialsProvider(accessKey, secretKey));
        }
    }
}
