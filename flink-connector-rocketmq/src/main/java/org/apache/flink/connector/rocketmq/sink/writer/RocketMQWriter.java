/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.sink.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.sink.InnerProducer;
import org.apache.flink.connector.rocketmq.sink.InnerProducerImpl;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContextImpl;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.streaming.connectors.rocketmq.common.selector.MessageQueueSelector;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RocketMQWriter<IN>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, SendCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);

    private final transient InnerProducer producer;

    private final DeliveryGuarantee deliveryGuarantee;
    private final MessageQueueSelector messageQueueSelector;
    private final RocketMQSinkContext rocketmqSinkContext;
    private final RocketMQSerializationSchema<IN> serializationSchema;
    private final Map<String, SendCommittable> committableMap;
    private final int sendRetryTimes;
    private final long sendSleepTimeMs;
    private final AtomicLong pendingRecords = new AtomicLong(0);
    /**
     * Bounds the in-flight async sends. {@code send.pending.max} was declared with exactly this
     * description but never read, so the send buffer grew without limit under a slow broker.
     */
    private final Semaphore sendPermits;

    private final AtomicReference<Throwable> asyncSendException = new AtomicReference<>();

    public RocketMQWriter(
            Configuration configuration,
            MessageQueueSelector messageQueueSelector,
            RocketMQSerializationSchema<IN> serializationSchema,
            Sink.InitContext initContext) {
        this(configuration, messageQueueSelector, serializationSchema, initContext, null);
    }

    /** @param producer the producer to send with, or null to create and start a real one */
    @VisibleForTesting
    RocketMQWriter(
            Configuration configuration,
            MessageQueueSelector messageQueueSelector,
            RocketMQSerializationSchema<IN> serializationSchema,
            Sink.InitContext initContext,
            @Nullable InnerProducer producer) {

        this.deliveryGuarantee =
                DeliveryGuarantee.valueOf(
                        configuration.getString(RocketMQSinkOptions.DELIVERY_GUARANTEE));
        this.messageQueueSelector = messageQueueSelector;
        this.serializationSchema = serializationSchema;
        this.rocketmqSinkContext = new RocketMQSinkContextImpl(initContext, configuration);
        this.committableMap = new ConcurrentHashMap<>();
        this.sendRetryTimes = configuration.getInteger(RocketMQSinkOptions.SEND_RETRY_TIMES);
        this.sendSleepTimeMs =
                configuration.getLong(RocketMQSinkOptions.OPTIONAL_WRITE_SLEEP_TIME_MS);
        this.sendPermits =
                new Semaphore(configuration.getInteger(RocketMQSinkOptions.SEND_PENDING_MAX));
        try {
            serializationSchema.open(
                    initContext.asSerializationSchemaInitializationContext(), rocketmqSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open serialization schema", e);
        }
        if (producer == null) {
            this.producer = new InnerProducerImpl(configuration);
            this.producer.start();
        } else {
            this.producer = producer;
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        checkAsyncException();
        Message message =
                serializationSchema.serialize(
                        element, rocketmqSinkContext, System.currentTimeMillis());

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            writeInTransaction(message);
            return;
        }

        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for send capacity", e);
        }
        pendingRecords.incrementAndGet();
        // The element is handed to the selector as its sharding argument, which is what lets a
        // HashMessageQueueSelector route records that share a key to the same queue.
        CompletableFuture<SendResult> sendFuture;
        try {
            sendFuture =
                    messageQueueSelector == null
                            ? producer.send(message)
                            : producer.send(message, messageQueueSelector, element);
        } catch (RuntimeException | Error e) {
            // A synchronously throwing send never reaches whenComplete, so the capacity and the
            // pending count have to be given back here or the writer would wedge at the limit.
            pendingRecords.decrementAndGet();
            sendPermits.release();
            throw e;
        }
        sendFuture.whenComplete(
                (sendResult, throwable) -> {
                    if (throwable != null) {
                        asyncSendException.compareAndSet(null, throwable);
                    }
                    pendingRecords.decrementAndGet();
                    sendPermits.release();
                });
    }

    private void writeInTransaction(Message message) throws IOException {
        int attempt = 0;
        Exception lastException = null;
        while (attempt <= sendRetryTimes) {
            try {
                SendResult sendResult = producer.sendMessageInTransaction(message).get();
                SendCommittable committable = new SendCommittable(sendResult);
                // Record the address the half message was actually accepted by. Only that node
                // holds the transaction context needed to end it, and the committer owns a
                // producer of its own whose route table may well resolve to a different node.
                committable.setBrokerAddress(producer.resolvePublishAddress(sendResult));
                committableMap.put(sendResult.getTransactionId(), committable);
                return; // success
            } catch (Exception e) {
                lastException = e;
                attempt++;
                if (attempt <= sendRetryTimes) {
                    LOG.warn(
                            "Send message failed (attempt {}/{}), retrying. topic={}",
                            attempt,
                            sendRetryTimes,
                            message.getTopic(),
                            e);
                    try {
                        Thread.sleep(sendSleepTimeMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during send retry", ie);
                    }
                }
            }
        }
        LOG.error(
                "Send message failed after {} retries, topic={}",
                sendRetryTimes,
                message.getTopic());
        throw new IOException(
                "Failed to send message after " + sendRetryTimes + " retries", lastException);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        while (pendingRecords.get() > 0) {
            checkAsyncException();
            Thread.sleep(10);
        }
        checkAsyncException();
    }

    private void checkAsyncException() throws IOException {
        Throwable throwable = asyncSendException.getAndSet(null);
        if (throwable != null) {
            throw new IOException("Async send message failed", throwable);
        }
    }

    @Override
    public Collection<SendCommittable> prepareCommit() throws IOException, InterruptedException {
        LOG.info("Prepare commit");
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            final List<SendCommittable> committables = new ArrayList<>(committableMap.values());
            LOG.info("Committable size={}.", committables.size());
            committableMap.clear();
            return committables;
        }
        return Collections.emptyList();
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        TwoPhaseCommittingSink.PrecommittingSinkWriter.super.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }
}
