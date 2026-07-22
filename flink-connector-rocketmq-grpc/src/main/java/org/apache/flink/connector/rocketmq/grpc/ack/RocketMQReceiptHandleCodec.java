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

import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.route.Address;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;

import java.util.Collections;

/**
 * The single adapter that bridges the connector's credential-free {@link RocketMQReceiptHandle} and
 * the {@code rocketmq-client-java} internal implementation classes. It is deliberately the
 * <b>only</b> class in the connector that depends on the SDK {@code
 * org.apache.rocketmq.client.java.*} internals, so that the (experimental) coupling is confined to
 * one place.
 *
 * <p>{@link #extract} runs on the source side and pulls the routing/ack fields out of an SDK
 * message view. {@link #toAckable} runs on the downstream ack side and rebuilds the minimal SDK
 * message view that {@code LiteSimpleConsumer#ack} / {@code changeInvisibleDuration} require (both
 * perform an {@code instanceof MessageViewImpl} downcast internally and only read the topic,
 * message id, receipt handle, lite topic and endpoints).
 */
@Internal
public final class RocketMQReceiptHandleCodec {

    private RocketMQReceiptHandleCodec() {}

    /**
     * Extract a credential-free receipt handle from an SDK message view.
     *
     * @param view the SDK message view returned by the consumer.
     * @param namespace the resource namespace configured on the source.
     * @param consumerGroup the consumer group configured on the source.
     * @return a self-contained {@link RocketMQReceiptHandle}.
     * @throws IllegalStateException if the view is not the expected internal implementation type.
     */
    public static RocketMQReceiptHandle extract(
            MessageView view, String namespace, String consumerGroup) {
        if (!(view instanceof MessageViewImpl)) {
            throw new IllegalStateException(
                    "Expected a "
                            + MessageViewImpl.class.getName()
                            + " but got "
                            + (view == null ? "null" : view.getClass().getName())
                            + "; the RocketMQ SDK internal message type has changed.");
        }
        final MessageViewImpl impl = (MessageViewImpl) view;
        final Endpoints endpoints = impl.getEndpoints();
        if (endpoints == null) {
            throw new IllegalStateException(
                    "The message view does not carry endpoints, cannot build a receipt handle for "
                            + impl.getMessageId());
        }
        return new RocketMQReceiptHandle(
                toEndpointString(endpoints),
                namespace,
                consumerGroup,
                impl.getTopic(),
                impl.getLiteTopic().orElse(null),
                impl.getMessageId().toString(),
                impl.getReceiptHandle(),
                impl.getDeliveryAttempt());
    }

    /**
     * Render the endpoints as a {@code host:port[;host:port...]} string that round-trips through
     * {@code new Endpoints(String)}. The {@code Endpoints#getFacade()} form is not used because it
     * carries a scheme prefix ({@code ipv4:}/{@code ipv6:}/{@code dns:}) that the string
     * constructor cannot parse back.
     */
    private static String toEndpointString(Endpoints endpoints) {
        final StringBuilder builder = new StringBuilder();
        for (Address address : endpoints.getAddresses()) {
            if (builder.length() > 0) {
                builder.append(';');
            }
            builder.append(address.getAddress());
        }
        return builder.toString();
    }

    /**
     * Rebuild the minimal SDK message view required to acknowledge or re-schedule the message
     * described by the given handle. The reconstructed view carries an empty body and no user
     * properties; only the fields consulted by the ack / changeInvisibleDuration RPC are populated.
     *
     * @param handle the credential-free receipt handle.
     * @return an SDK {@link MessageView} suitable for {@code ack} / {@code
     *     changeInvisibleDuration}.
     */
    public static MessageView toAckable(RocketMQReceiptHandle handle) {
        final MessageId messageId = MessageIdCodec.getInstance().decode(handle.getMessageId());
        final MessageQueueImpl messageQueue = buildMessageQueue(handle);
        return new MessageViewImpl(
                messageId,
                handle.getTopic(),
                new byte[0],
                null,
                null,
                handle.getLiteTopic(),
                null,
                null,
                Collections.emptyList(),
                Collections.emptyMap(),
                "",
                0L,
                handle.getDeliveryAttempt(),
                messageQueue,
                handle.getReceiptHandle(),
                0L,
                false,
                null);
    }

    private static MessageQueueImpl buildMessageQueue(RocketMQReceiptHandle handle) {
        final apache.rocketmq.v2.Endpoints endpoints =
                new Endpoints(handle.getEndpoint()).toProtobuf();
        final apache.rocketmq.v2.Broker broker =
                apache.rocketmq.v2.Broker.newBuilder()
                        .setName("")
                        .setId(0)
                        .setEndpoints(endpoints)
                        .build();
        final apache.rocketmq.v2.Resource topic =
                apache.rocketmq.v2.Resource.newBuilder()
                        .setResourceNamespace(handle.getNamespace())
                        .setName(handle.getTopic())
                        .build();
        // A concrete permission is required: MessageQueueImpl rejects PERMISSION_UNSPECIFIED.
        final apache.rocketmq.v2.MessageQueue messageQueue =
                apache.rocketmq.v2.MessageQueue.newBuilder()
                        .setTopic(topic)
                        .setId(0)
                        .setPermission(apache.rocketmq.v2.Permission.READ)
                        .setBroker(broker)
                        .build();
        return new MessageQueueImpl(messageQueue);
    }
}
