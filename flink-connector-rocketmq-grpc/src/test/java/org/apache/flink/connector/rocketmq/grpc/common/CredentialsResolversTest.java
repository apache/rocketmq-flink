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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CredentialsResolvers}. */
class CredentialsResolversTest {

    private static final SessionCredentialsProvider RESOLVED_PROVIDER =
            new StaticSessionCredentialsProvider("resolved-ak", "resolved-sk");

    @Test
    void createFromConfigurationReturnsNullWithoutResolverClass() {
        assertThat(CredentialsResolvers.createFromConfiguration(new Configuration())).isNull();
    }

    @Test
    void createFromConfigurationInstantiatesAndConfiguresTheResolver() {
        final Configuration configuration = new Configuration();
        configuration.set(
                RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS, RecordingResolver.class.getName());

        final CredentialsResolver resolver =
                CredentialsResolvers.createFromConfiguration(configuration);

        assertThat(resolver).isInstanceOf(RecordingResolver.class);
        assertThat(((RecordingResolver) resolver).configured).isSameAs(configuration);
    }

    @Test
    void createFromConfigurationFailsForNonResolverClass() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS, String.class.getName());

        assertThatThrownBy(() -> CredentialsResolvers.createFromConfiguration(configuration))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(String.class.getName());
    }

    @Test
    void createFromConfigurationFailsForMissingClass() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS, "does.not.Exist");

        assertThatThrownBy(() -> CredentialsResolvers.createFromConfiguration(configuration))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("does.not.Exist");
    }

    @Test
    void resolverTakesPrecedenceOverStaticKeys() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ACCESS_KEY, "static-ak");
        configuration.set(RocketMQGrpcOptions.SECRET_KEY, "static-sk");
        final RecordingResolver resolver = new RecordingResolver();

        final ClientConfigurationBuilder builder = newBuilder();
        CredentialsResolvers.applyCredentials(builder, "host-a:8080", configuration, resolver);

        assertThat(resolver.resolvedEndpoint).isEqualTo("host-a:8080");
        assertThat(builder.build().getCredentialsProvider()).contains(RESOLVED_PROVIDER);
    }

    @Test
    void staticKeysAreUsedWithoutAResolver() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ACCESS_KEY, "static-ak");
        configuration.set(RocketMQGrpcOptions.SECRET_KEY, "static-sk");

        final ClientConfigurationBuilder builder = newBuilder();
        CredentialsResolvers.applyCredentials(builder, "host-a:8080", configuration, null);

        assertThat(builder.build().getCredentialsProvider())
                .containsInstanceOf(StaticSessionCredentialsProvider.class);
    }

    @Test
    void noCredentialsAreSetWhenTheResolverReturnsNull() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ACCESS_KEY, "static-ak");
        configuration.set(RocketMQGrpcOptions.SECRET_KEY, "static-sk");

        final ClientConfigurationBuilder builder = newBuilder();
        CredentialsResolvers.applyCredentials(
                builder, "host-a:8080", configuration, endpoint -> null);

        // The resolver explicitly resolved "no credentials"; the static keys must not be used.
        assertThat(builder.build().getCredentialsProvider()).isEmpty();
    }

    @Test
    void noCredentialsAreSetWithoutResolverAndWithIncompleteStaticKeys() {
        final Configuration configuration = new Configuration();
        configuration.set(RocketMQGrpcOptions.ACCESS_KEY, "static-ak");

        final ClientConfigurationBuilder builder = newBuilder();
        CredentialsResolvers.applyCredentials(builder, "host-a:8080", configuration, null);

        assertThat(builder.build().getCredentialsProvider()).isEmpty();
    }

    private static ClientConfigurationBuilder newBuilder() {
        return ClientConfiguration.newBuilder().setEndpoints("host-a:8080");
    }

    /** A resolver that records its interactions for assertions. */
    public static class RecordingResolver implements CredentialsResolver {

        @Nullable private Configuration configured;
        @Nullable private String resolvedEndpoint;

        @Override
        public void configure(Configuration configuration) {
            this.configured = configuration;
        }

        @Override
        public SessionCredentialsProvider resolve(String endpoint) {
            this.resolvedEndpoint = endpoint;
            return RESOLVED_PROVIDER;
        }
    }
}
