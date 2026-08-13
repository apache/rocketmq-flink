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
import org.apache.flink.util.StringUtils;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;

import javax.annotation.Nullable;

/**
 * Builds an SDK {@link ClientConfiguration} from a Flink {@link Configuration}. The produced
 * configuration is shared by the source ({@code SimpleConsumer}) and the sink ({@code Producer}).
 */
@Internal
public class ClientConfigurationProvider {

    private ClientConfigurationProvider() {}

    /** Build a {@link ClientConfiguration} from the given Flink configuration. */
    public static ClientConfiguration getClientConfiguration(Configuration configuration) {
        final String endpoints = configuration.get(RocketMQGrpcOptions.ENDPOINTS);
        if (StringUtils.isNullOrWhitespaceOnly(endpoints)) {
            throw new IllegalArgumentException(
                    "The endpoints of the RocketMQ gRPC connector must be configured.");
        }
        return getClientConfiguration(
                configuration,
                endpoints,
                configuration.get(RocketMQGrpcOptions.NAMESPACE),
                CredentialsResolvers.createFromConfiguration(configuration));
    }

    /**
     * Build a {@link ClientConfiguration} whose endpoints and namespace are supplied by the caller
     * (e.g. taken from a receipt handle), while the credentials, TLS and request timeout come from
     * the Flink configuration.
     */
    public static ClientConfiguration getClientConfiguration(
            Configuration configuration,
            String endpoints,
            @Nullable String namespace,
            @Nullable CredentialsResolver credentialsResolver) {
        final ClientConfigurationBuilder builder =
                ClientConfiguration.newBuilder()
                        .setEndpoints(endpoints)
                        .setRequestTimeout(configuration.get(RocketMQGrpcOptions.REQUEST_TIMEOUT))
                        .enableSsl(configuration.get(RocketMQGrpcOptions.TLS_ENABLED));

        if (!StringUtils.isNullOrWhitespaceOnly(namespace)) {
            builder.setNamespace(namespace);
        }

        CredentialsResolvers.applyCredentials(
                builder, endpoints, configuration, credentialsResolver);

        return builder.build();
    }
}
