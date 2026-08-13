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

package org.apache.flink.connector.rocketmq.grpc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Shared configuration options for the RocketMQ gRPC connector. These options describe how the
 * underlying {@code rocketmq-client-java} SDK connects to the RocketMQ proxy (endpoints,
 * credentials, TLS and namespace) and are consumed by both the source and the sink.
 *
 * <p>These are the programmatic/SDK-facing options used by the source and sink builders and their
 * runtime; every key carries the {@link #CLIENT_CONFIG_PREFIX} prefix. The SQL DDL keys are defined
 * separately in {@code RocketMQGrpcConnectorOptions}.
 */
@PublicEvolving
public class RocketMQGrpcOptions {

    private RocketMQGrpcOptions() {}

    /** Prefix for the shared RocketMQ gRPC client options. */
    public static final String CLIENT_CONFIG_PREFIX = "rocketmq.client.";

    public static final ConfigOption<String> ENDPOINTS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "endpoints")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The access point (proxy) endpoints the gRPC SDK communicates with, "
                                    + "for example '127.0.0.1:8080'.");

    public static final ConfigOption<String> NAMESPACE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "namespace")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The resource namespace of the RocketMQ instance.");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "access-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access key used for static session credentials.");

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "secret-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret key used for static session credentials.");

    /**
     * The fully qualified class name of a {@link
     * org.apache.flink.connector.rocketmq.grpc.common.CredentialsResolver} implementation (public,
     * with a public no-argument constructor).
     *
     * <p>The resolver is instantiated reflectively on each TaskManager (never serialized) and
     * resolves session credentials per proxy endpoint locally, e.g. from environment variables,
     * mounted secret files or an external KMS. This keeps plaintext secrets out of the job graph
     * and lets one downstream ack operator serve handles from multiple clusters with different
     * credentials. When set, it takes precedence over {@link #ACCESS_KEY} / {@link #SECRET_KEY};
     * returning {@code null} for an endpoint means no authentication is required.
     */
    public static final ConfigOption<String> CREDENTIALS_RESOLVER_CLASS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "credentials-resolver-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The fully qualified class name of a CredentialsResolver that resolves "
                                    + "session credentials per endpoint locally on the TaskManager "
                                    + "(for example from environment variables, mounted secret files "
                                    + "or an external KMS). When set, it takes precedence over the "
                                    + "static access-key/secret-key options and keeps plaintext "
                                    + "credentials out of the job graph.");

    public static final ConfigOption<Boolean> TLS_ENABLED =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tls-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether TLS is enabled for the gRPC transport.");

    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "request-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(3))
                    .withDescription("The request timeout for a single gRPC invocation.");
}
