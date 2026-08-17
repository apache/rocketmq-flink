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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandleCodec;
import org.apache.flink.connector.rocketmq.grpc.source.ConsumerMode;
import org.apache.flink.connector.rocketmq.grpc.source.InvisibleDurationRenewalPolicies;
import org.apache.flink.connector.rocketmq.grpc.source.InvisibleDurationRenewalPolicy;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.client.apis.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link SplitReader} implementation for the RocketMQ gRPC Pop model.
 *
 * <p>Because the broker performs message-level load balancing across the consumer group, this
 * reader does not partition topics. Instead every subtask consumes the same topic: the main lite
 * topic in {@link ConsumerMode#LITE}, or the subscribed normal topic in {@link
 * ConsumerMode#SIMPLE}. To raise a single subtask's throughput it runs {@code fetch-concurrency}
 * worker threads that share a single {@link PopConsumer}, each issuing a blocking {@code
 * receive()} long poll. Received messages are handed to the (single) fetcher thread through an
 * internal queue.
 *
 * <p>Acknowledgement depends on the mode. In {@link ConsumerMode#LITE} this reader never
 * acknowledges messages: acknowledgement is deferred to a downstream operator that receives the
 * {@code AckableMessage} records carrying a self-contained receipt handle. In {@link
 * ConsumerMode#SIMPLE} the source reader acknowledges through {@link #ack(RocketMQReceiptHandle)}
 * on this very consumer once the checkpoint that observed the messages completes. Messages that are
 * never acked become visible again after their invisible duration and are redelivered by the
 * broker, which provides the at-least-once guarantee.
 *
 * <p>When an {@link InvisibleDurationRenewalPolicy} is configured, messages that are still buffered
 * in the internal queue shortly before they would become visible again are offered to the policy,
 * which may extend their invisible duration. Messages already handed to the record emitter are
 * never renewed because renewing refreshes the receipt handle carried downstream.
 */
@Internal
public class RocketMQGrpcSourceSplitReader
        implements SplitReader<MessageViewImpl, RocketMQGrpcSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQGrpcSourceSplitReader.class);

    private static final long RECEIVE_BACKOFF_INITIAL_MS = 100L;
    private static final long RECEIVE_BACKOFF_MAX_MS = 30_000L;

    private final Configuration configuration;
    private final int fetchConcurrency;
    private final int maxMessageNum;
    private final Duration invisibleDuration;
    private final Duration renewalAheadTime;
    @Nullable private final InvisibleDurationRenewalPolicy renewalPolicy;

    private final BlockingQueue<MessageViewImpl> elementQueue;
    private final AtomicBoolean wakeup = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile boolean closed = false;
    private ReceiveWorker[] workers;

    /**
     * Written by the fetcher thread in {@link #startWorkers()} and read by the receive workers, the
     * renewal thread and the mailbox thread ({@link #ack(RocketMQReceiptHandle)}), hence volatile.
     */
    private volatile PopConsumer consumer;

    @Nullable private ScheduledThreadPoolExecutor renewalExecutor;

    public RocketMQGrpcSourceSplitReader(Configuration configuration) {
        this.configuration = configuration;
        this.fetchConcurrency = configuration.get(RocketMQGrpcSourceOptions.FETCH_CONCURRENCY);
        this.maxMessageNum = configuration.get(RocketMQGrpcSourceOptions.MAX_MESSAGE_NUM);
        this.invisibleDuration = configuration.get(RocketMQGrpcSourceOptions.INVISIBLE_DURATION);
        this.renewalAheadTime = configuration.get(RocketMQGrpcSourceOptions.RENEWAL_AHEAD_TIME);
        this.renewalPolicy =
                InvisibleDurationRenewalPolicies.createFromConfiguration(configuration);
        if (renewalPolicy != null) {
            checkArgument(
                    !renewalAheadTime.isNegative() && !renewalAheadTime.isZero(),
                    "renewal-ahead-time must be positive");
            checkArgument(
                    renewalAheadTime.compareTo(invisibleDuration) < 0,
                    "renewal-ahead-time (%s) must be smaller than the invisible duration (%s)",
                    renewalAheadTime,
                    invisibleDuration);
        }
        this.elementQueue = new ArrayBlockingQueue<>(maxMessageNum * fetchConcurrency);
    }

    @Override
    public RecordsWithSplitIds<MessageViewImpl> fetch() {
        if (wakeup.compareAndSet(true, false) || closed) {
            return new RecordsBySplits.Builder<MessageViewImpl>().build();
        }

        final MessageViewImpl first;
        try {
            first = elementQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new RecordsBySplits.Builder<MessageViewImpl>().build();
        }
        if (first == null) {
            return new RecordsBySplits.Builder<MessageViewImpl>().build();
        }

        final List<MessageViewImpl> batch = new ArrayList<>(maxMessageNum);
        batch.add(first);
        elementQueue.drainTo(batch, maxMessageNum - 1);

        final RecordsBySplits.Builder<MessageViewImpl> builder = new RecordsBySplits.Builder<>();
        for (MessageViewImpl message : batch) {
            // From here on the receipt handle travels downstream, so freeze it: waits for an
            // in-flight renewal to finish and prevents any further renewal.
            message.markEmitted();
            builder.add(RocketMQGrpcSourceSplit.SPLIT_ID, message);
        }
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQGrpcSourceSplit> splitsChanges) {
        // The placeholder split simply triggers the reader to start consuming.
        if (!splitsChanges.splits().isEmpty()) {
            startWorkers();
        }
    }

    private void startWorkers() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            consumer = PopConsumerProvider.create(configuration);
        } catch (ClientException e) {
            started.set(false);
            throw new FlinkRuntimeException("Failed to create the RocketMQ gRPC consumer", e);
        }
        if (renewalPolicy != null) {
            renewalExecutor =
                    new ScheduledThreadPoolExecutor(
                            1,
                            runnable -> {
                                final Thread thread = new Thread(runnable, "rocketmq-grpc-renewal");
                                thread.setDaemon(true);
                                return thread;
                            });
            renewalExecutor.setRemoveOnCancelPolicy(true);
        }
        workers = new ReceiveWorker[fetchConcurrency];
        for (int i = 0; i < fetchConcurrency; i++) {
            final ReceiveWorker worker = new ReceiveWorker(i);
            workers[i] = worker;
            worker.start();
        }
        final boolean simple =
                configuration.get(RocketMQGrpcSourceOptions.MODE) == ConsumerMode.SIMPLE;
        LOG.info(
                "Started {} receive worker(s) sharing one {} consumer bound to topic {}",
                fetchConcurrency,
                simple ? "simple" : "lite",
                configuration.get(
                        simple
                                ? RocketMQGrpcSourceOptions.TOPIC
                                : RocketMQGrpcSourceOptions.MAIN_TOPIC));
    }

    /**
     * Acknowledge a message that this reader received, removing it from the Pop invisible set. Used
     * by the source reader in SIMPLE mode once the checkpoint that observed the message completes.
     *
     * <p>The credential-free {@link RocketMQReceiptHandle} is passed instead of the SDK {@code
     * MessageView} on purpose: it keeps the checkpoint ack tracker from pinning message bodies in
     * memory, and the minimal view the ack RPC needs is rebuilt here.
     *
     * <p>SDK Pop consumers are thread-safe, so acking from the mailbox thread while the receive
     * workers block in {@code receive()} on the same instance is safe.
     *
     * @param handle the receipt handle of a message emitted by this reader.
     * @throws ClientException if the ack RPC fails.
     * @throws IllegalStateException if the consumer has not been started yet or is already closed.
     */
    public void ack(RocketMQReceiptHandle handle) throws ClientException {
        final PopConsumer currentConsumer = consumer;
        if (currentConsumer == null || closed) {
            throw new IllegalStateException(
                    "Cannot acknowledge message "
                            + handle.getMessageId()
                            + ": the RocketMQ gRPC consumer is "
                            + (currentConsumer == null ? "not started yet" : "already closed"));
        }
        currentConsumer.ack(RocketMQReceiptHandleCodec.toAckable(handle));
    }

    /** Inject a consumer so that ack / renewal behaviour can be tested without a live cluster. */
    @VisibleForTesting
    void setConsumer(PopConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void wakeUp() {
        wakeup.compareAndSet(false, true);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (renewalExecutor != null) {
            renewalExecutor.shutdownNow();
        }
        if (workers != null) {
            for (ReceiveWorker worker : workers) {
                worker.shutdown();
            }
        }
        // Close the consumer before joining the workers so that a worker blocked in a receive()
        // long poll fails fast instead of holding its thread until the await duration elapses.
        if (consumer != null) {
            consumer.close();
        }
        if (workers != null) {
            for (ReceiveWorker worker : workers) {
                worker.join(TimeUnit.SECONDS.toMillis(10));
            }
        }
    }

    private void scheduleRenewal(MessageViewImpl message, int renewalCount, Duration current) {
        if (renewalExecutor == null || closed) {
            return;
        }
        final long delayMs = Math.max(current.minus(renewalAheadTime).toMillis(), 0L);
        try {
            final ScheduledFuture<?> future =
                    renewalExecutor.schedule(
                            () -> renew(message, renewalCount), delayMs, TimeUnit.MILLISECONDS);
            message.setRenewalFuture(future);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // The reader is closing; the message will simply become visible again.
        }
    }

    private void renew(MessageViewImpl message, int renewalCount) {
        synchronized (message) {
            if (closed || message.isEmitted()) {
                return;
            }
            final Duration next;
            try {
                next = renewalPolicy.renew(message, renewalCount);
            } catch (Exception e) {
                LOG.warn(
                        "The renewal policy failed for message {}; it will become visible again",
                        message.getMessageId(),
                        e);
                return;
            }
            if (next == null || next.isZero() || next.isNegative()) {
                return;
            }
            try {
                consumer.changeInvisibleDuration(message.getMessageView(), next);
            } catch (ClientException e) {
                LOG.warn(
                        "Failed to renew the invisible duration of message {}; it will become "
                                + "visible again",
                        message.getMessageId(),
                        e);
                return;
            }
            scheduleRenewal(message, renewalCount + 1, next);
        }
    }

    /**
     * A single receive loop over the shared {@link PopConsumer}. Because the consumer is
     * thread-safe, several workers can call {@code receive()} on it concurrently; a blocking {@code
     * receive()} only occupies its calling thread, which is why several threads are used to keep
     * that many long polls in flight.
     */
    private class ReceiveWorker extends Thread {

        private final int index;

        private volatile boolean running = true;

        private ReceiveWorker(int index) {
            super("rocketmq-grpc-receive-worker-" + index);
            this.index = index;
        }

        private void shutdown() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            long backoffMs = RECEIVE_BACKOFF_INITIAL_MS;
            while (running && !closed) {
                try {
                    final List<org.apache.rocketmq.client.apis.message.MessageView> messages =
                            consumer.receive(maxMessageNum, invisibleDuration);
                    backoffMs = RECEIVE_BACKOFF_INITIAL_MS;
                    if (!running || closed) {
                        // Dropped messages are redelivered after their invisible duration.
                        break;
                    }
                    for (org.apache.rocketmq.client.apis.message.MessageView message : messages) {
                        final MessageViewImpl view = new MessageViewImpl(message);
                        scheduleRenewal(view, 0, invisibleDuration);
                        elementQueue.put(view);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (ClientException e) {
                    if (running && !closed) {
                        LOG.warn(
                                "Receive worker {} failed to receive messages, retrying in {} ms",
                                index,
                                backoffMs,
                                e);
                        try {
                            Thread.sleep(backoffMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        backoffMs = Math.min(backoffMs * 2, RECEIVE_BACKOFF_MAX_MS);
                    }
                }
            }
        }
    }
}
