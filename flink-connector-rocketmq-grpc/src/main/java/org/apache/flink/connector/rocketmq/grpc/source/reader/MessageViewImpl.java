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

package org.apache.flink.connector.rocketmq.grpc.source.reader;

import org.apache.flink.annotation.Internal;

import org.apache.rocketmq.client.apis.message.MessageId;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The {@link MessageView} implementation backed by an SDK {@link
 * org.apache.rocketmq.client.apis.message.MessageView}. It retains the underlying SDK message so
 * that the reader can acknowledge it after a checkpoint completes.
 *
 * <p>It additionally tracks whether the message has been handed to the record emitter. Renewing the
 * invisible duration refreshes the SDK receipt handle, so a renewal and the handle extraction at
 * emit time must be mutually exclusive: both happen under the monitor of this instance, and once
 * {@link #markEmitted()} has been called no further renewal is attempted.
 */
@Internal
public class MessageViewImpl implements MessageView {

    private final org.apache.rocketmq.client.apis.message.MessageView messageView;
    private final byte[] body;

    private boolean emitted;
    private Future<?> renewalFuture;

    public MessageViewImpl(org.apache.rocketmq.client.apis.message.MessageView messageView) {
        this.messageView = messageView;
        final ByteBuffer buffer = messageView.getBody();
        this.body = new byte[buffer.remaining()];
        buffer.get(this.body);
    }

    /** The underlying SDK message, used for acknowledgement via the {@code SimpleConsumer}. */
    public org.apache.rocketmq.client.apis.message.MessageView getMessageView() {
        return messageView;
    }

    /**
     * Mark this message as handed to the record emitter and cancel any pending renewal. Blocks
     * while a renewal RPC for this message is in flight so that the receipt handle extracted
     * afterwards is stable.
     */
    public synchronized void markEmitted() {
        emitted = true;
        if (renewalFuture != null) {
            renewalFuture.cancel(false);
            renewalFuture = null;
        }
    }

    /** Whether this message has been handed to the record emitter. */
    public synchronized boolean isEmitted() {
        return emitted;
    }

    /** Track the pending renewal task so that {@link #markEmitted()} can cancel it. */
    public synchronized void setRenewalFuture(Future<?> future) {
        if (emitted) {
            future.cancel(false);
        } else {
            this.renewalFuture = future;
        }
    }

    @Override
    public String getMessageId() {
        final MessageId messageId = messageView.getMessageId();
        return messageId == null ? null : messageId.toString();
    }

    @Override
    public String getTopic() {
        return messageView.getTopic();
    }

    @Override
    public String getTag() {
        return messageView.getTag().orElse(null);
    }

    @Override
    public Collection<String> getKeys() {
        return messageView.getKeys();
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public int getDeliveryAttempt() {
        return messageView.getDeliveryAttempt();
    }

    @Override
    public long getEventTime() {
        return messageView.getBornTimestamp();
    }

    @Override
    public Map<String, String> getProperties() {
        return messageView.getProperties();
    }
}
