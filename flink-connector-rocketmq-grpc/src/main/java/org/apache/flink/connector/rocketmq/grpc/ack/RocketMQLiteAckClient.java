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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.connector.rocketmq.grpc.common.ClientConfigurationProvider;
import org.apache.flink.connector.rocketmq.grpc.common.CredentialsResolver;
import org.apache.flink.connector.rocketmq.grpc.common.CredentialsResolvers;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A credential-free acknowledgement client that a downstream operator uses to acknowledge or
 * re-schedule RocketMQ Pop messages described by a {@link RocketMQReceiptHandle}. The credentials
 * used to talk to the RocketMQ proxy are configured on the operator that owns the client and are
 * <b>never</b> carried in the data stream.
 *
 * <p>The client keeps a lazily populated pool of same-group {@link LiteSimpleConsumer}s, one per
 * {@link ConsumerKey routing triple} carried by the incoming handles, and issues the {@code ack} /
 * {@code changeInvisibleDuration} RPC through the consumer that matches the handle. The pooled
 * consumer for a handle is built with a {@link ClientConfiguration} whose endpoints and namespace
 * come from the handle (so the ack RPC is routed to the right proxy and resource) and whose
 * credentials/TLS/timeout come from the operator {@link Configuration}. This is required because
 * the SDK stamps the ack request with the <em>consumer's</em> namespace, so a pooled consumer must
 * share the handle's namespace.
 *
 * <p>Instances are shared per TaskManager JVM: {@link #acquire(Configuration)} reference-counts a
 * client per distinct client configuration and {@link #release(Configuration)} closes it once no
 * operator instance references it anymore, so callers must not close a client directly.
 */
@Internal
public final class RocketMQLiteAckClient {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQLiteAckClient.class);

    /**
     * The await duration is only consulted by {@code receive}, which this ack-only client never
     * calls; a small non-null value is enough to satisfy the builder.
     */
    private static final Duration ACK_ONLY_AWAIT_DURATION = Duration.ofSeconds(5);

    private static final int MAX_RPC_ATTEMPTS = 3;
    private static final long RPC_RETRY_BACKOFF_INITIAL_MS = 100L;

    /** {@code apache.rocketmq.v2.Code.INVALID_RECEIPT_HANDLE}, embedded in exception messages. */
    private static final String INVALID_RECEIPT_HANDLE_MARKER = "response-code=40013";

    private static final Map<String, RefCounted> CLIENTS = new HashMap<>();

    private final Configuration configuration;
    @Nullable private final CredentialsResolver credentialsResolver;
    private final Map<ConsumerKey, LiteSimpleConsumer> consumers = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    private RocketMQLiteAckClient(Configuration configuration) {
        this.configuration =
                Objects.requireNonNull(configuration, "configuration should not be null");
        this.credentialsResolver = CredentialsResolvers.createFromConfiguration(configuration);
    }

    /**
     * Acquire the shared ack client for the given client configuration, creating it if necessary
     * and incrementing its reference count.
     *
     * @param configuration the operator configuration carrying the RocketMQ client options.
     * @return the shared ack client; callers must not close it directly.
     */
    public static synchronized RocketMQLiteAckClient acquire(Configuration configuration) {
        Objects.requireNonNull(configuration, "configuration should not be null");
        final String key = keyOf(configuration);
        RefCounted ref = CLIENTS.get(key);
        if (ref == null) {
            ref = new RefCounted(new RocketMQLiteAckClient(configuration));
            CLIENTS.put(key, ref);
        }
        ref.count++;
        return ref.client;
    }

    /**
     * Release a previously {@link #acquire(Configuration) acquired} client, decrementing its
     * reference count and closing it once no operator instance references it anymore.
     *
     * <p>The client is closed outside the registry lock so that a slow close (it issues network
     * calls) cannot block concurrent {@code acquire}/{@code release} calls of other operators.
     *
     * @param configuration the same configuration that was passed to {@link
     *     #acquire(Configuration)}.
     */
    public static void release(Configuration configuration) {
        Objects.requireNonNull(configuration, "configuration should not be null");
        final String key = keyOf(configuration);
        RocketMQLiteAckClient toClose = null;
        synchronized (RocketMQLiteAckClient.class) {
            final RefCounted ref = CLIENTS.get(key);
            if (ref == null) {
                return;
            }
            ref.count--;
            if (ref.count <= 0) {
                CLIENTS.remove(key);
                toClose = ref.client;
            }
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    @VisibleForTesting
    static synchronized int getReferenceCount(Configuration configuration) {
        final RefCounted ref = CLIENTS.get(keyOf(configuration));
        return ref == null ? 0 : ref.count;
    }

    /**
     * Build a stable pool key from the credential-bearing client options. Distinct credentials,
     * endpoints, namespaces, TLS settings or timeouts each get their own shared client. The secret
     * key is hashed so the plaintext secret is never held in the registry key.
     */
    private static String keyOf(Configuration configuration) {
        return configuration.get(RocketMQGrpcOptions.ENDPOINTS)
                + '|'
                + configuration.get(RocketMQGrpcOptions.NAMESPACE)
                + '|'
                + configuration.get(RocketMQGrpcOptions.ACCESS_KEY)
                + '|'
                + sha256(configuration.get(RocketMQGrpcOptions.SECRET_KEY))
                + '|'
                + configuration.get(RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS)
                + '|'
                + configuration.get(RocketMQGrpcOptions.TLS_ENABLED)
                + '|'
                + configuration.get(RocketMQGrpcOptions.REQUEST_TIMEOUT);
    }

    private static String sha256(@Nullable String value) {
        if (value == null) {
            return "";
        }
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return StringUtils.byteToHexString(
                    digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new FlinkRuntimeException("SHA-256 is not available.", e);
        }
    }

    /**
     * Acknowledge the message described by the handle, removing it from the Pop invisible set so it
     * is not re-delivered.
     *
     * @param handle the credential-free receipt handle of the message to acknowledge.
     */
    public void ack(RocketMQReceiptHandle handle) {
        Objects.requireNonNull(handle, "handle should not be null");
        final LiteSimpleConsumer consumer = getOrCreateConsumer(handle);
        invokeWithRetry(
                "acknowledge",
                handle,
                () -> consumer.ack(RocketMQReceiptHandleCodec.toAckable(handle)));
    }

    /**
     * Change the invisible duration of the message described by the handle. Extending the invisible
     * duration defers the next possible re-delivery of the message, which downstream operators use
     * to throttle a (sub) topic without dropping the message.
     *
     * @param handle the credential-free receipt handle of the message.
     * @param invisibleDuration the new invisible duration counted from now.
     */
    public void changeInvisibleDuration(RocketMQReceiptHandle handle, Duration invisibleDuration) {
        Objects.requireNonNull(handle, "handle should not be null");
        Objects.requireNonNull(invisibleDuration, "invisibleDuration should not be null");
        final LiteSimpleConsumer consumer = getOrCreateConsumer(handle);
        invokeWithRetry(
                "change the invisible duration of",
                handle,
                () ->
                        consumer.changeInvisibleDuration(
                                RocketMQReceiptHandleCodec.toAckable(handle), invisibleDuration));
    }

    /**
     * Run the RPC with a bounded exponential-backoff retry. An expired/invalid receipt handle is
     * tolerated: the message will simply be redelivered by the broker (at-least-once), so failing
     * the job for it would be worse than the duplicate.
     */
    private void invokeWithRetry(String action, RocketMQReceiptHandle handle, AckRpc rpc) {
        long backoffMs = RPC_RETRY_BACKOFF_INITIAL_MS;
        for (int attempt = 1; ; attempt++) {
            try {
                rpc.run();
                return;
            } catch (ClientException e) {
                if (isInvalidReceiptHandle(e)) {
                    LOG.warn(
                            "The receipt handle of message {} is invalid or expired; the message "
                                    + "will be redelivered by the broker.",
                            handle.getMessageId(),
                            e);
                    return;
                }
                if (attempt >= MAX_RPC_ATTEMPTS) {
                    throw new FlinkRuntimeException(
                            "Failed to "
                                    + action
                                    + " RocketMQ message "
                                    + handle.getMessageId()
                                    + " after "
                                    + attempt
                                    + " attempts",
                            e);
                }
                LOG.warn(
                        "Failed to {} RocketMQ message {} (attempt {}/{}), retrying in {} ms",
                        action,
                        handle.getMessageId(),
                        attempt,
                        MAX_RPC_ATTEMPTS,
                        backoffMs,
                        e);
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    final FlinkRuntimeException interrupted =
                            new FlinkRuntimeException(
                                    "Interrupted while retrying to "
                                            + action
                                            + " RocketMQ message "
                                            + handle.getMessageId(),
                                    ie);
                    interrupted.addSuppressed(e);
                    throw interrupted;
                }
                backoffMs *= 2;
            }
        }
    }

    private static boolean isInvalidReceiptHandle(ClientException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            final String message = t.getMessage();
            if (message != null && message.contains(INVALID_RECEIPT_HANDLE_MARKER)) {
                return true;
            }
        }
        return false;
    }

    /** An ack-related RPC over a pooled consumer. */
    @FunctionalInterface
    private interface AckRpc {
        void run() throws ClientException;
    }

    /** Close all pooled consumers. Invoked by {@link #release(Configuration)} on last release. */
    private void close() {
        closed = true;
        for (Map.Entry<ConsumerKey, LiteSimpleConsumer> entry : consumers.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.warn("Failed to close the ack consumer for {}.", entry.getKey(), e);
            }
        }
        consumers.clear();
    }

    private LiteSimpleConsumer getOrCreateConsumer(RocketMQReceiptHandle handle) {
        if (closed) {
            throw new IllegalStateException("The ack client has been closed.");
        }
        final ConsumerKey key =
                new ConsumerKey(
                        handle.getEndpoint(), handle.getNamespace(), handle.getConsumerGroup());
        return consumers.computeIfAbsent(key, k -> createConsumer(k, handle.getTopic()));
    }

    private LiteSimpleConsumer createConsumer(ConsumerKey key, String bindTopic) {
        try {
            final ClientServiceProvider provider = ClientServiceProvider.loadService();
            return provider.newLiteSimpleConsumerBuilder()
                    .setClientConfiguration(
                            ClientConfigurationProvider.getClientConfiguration(
                                    configuration,
                                    key.endpoint,
                                    key.namespace,
                                    credentialsResolver))
                    .setConsumerGroup(key.consumerGroup)
                    .setAwaitDuration(ACK_ONLY_AWAIT_DURATION)
                    .bindTopic(bindTopic)
                    .build();
        } catch (ClientException e) {
            throw new FlinkRuntimeException("Failed to create the ack consumer for " + key, e);
        }
    }

    private static final class RefCounted {
        private final RocketMQLiteAckClient client;
        private int count;

        private RefCounted(RocketMQLiteAckClient client) {
            this.client = client;
        }
    }

    /**
     * The identity of a pooled ack consumer: a message can only be acknowledged by a consumer in
     * the same group, pointing at the same proxy endpoint(s) and resource namespace.
     */
    private static final class ConsumerKey {

        private final String endpoint;
        private final String namespace;
        private final String consumerGroup;

        private ConsumerKey(String endpoint, String namespace, String consumerGroup) {
            this.endpoint = endpoint;
            this.namespace = namespace;
            this.consumerGroup = consumerGroup;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConsumerKey that = (ConsumerKey) o;
            return endpoint.equals(that.endpoint)
                    && namespace.equals(that.namespace)
                    && consumerGroup.equals(that.consumerGroup);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoint, namespace, consumerGroup);
        }

        @Override
        public String toString() {
            return "ConsumerKey{"
                    + "endpoint='"
                    + endpoint
                    + '\''
                    + ", namespace='"
                    + namespace
                    + '\''
                    + ", consumerGroup='"
                    + consumerGroup
                    + '\''
                    + '}';
        }
    }
}
