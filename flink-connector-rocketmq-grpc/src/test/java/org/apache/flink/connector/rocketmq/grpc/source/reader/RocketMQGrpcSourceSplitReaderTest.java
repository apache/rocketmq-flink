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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.ForbiddenException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.NotFoundException;
import org.apache.rocketmq.client.java.exception.ProxyTimeoutException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.exception.UnauthorizedException;
import org.apache.rocketmq.client.java.exception.UnsupportedException;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the SIMPLE-mode acknowledgement path of {@link RocketMQGrpcSourceSplitReader}. */
class RocketMQGrpcSourceSplitReaderTest {

    /** Status codes of the gRPC protocol, see {@code apache.rocketmq.v2.Code}. */
    private static final int ILLEGAL_CONSUMER_GROUP = 40003;

    private static final int UNAUTHORIZED = 40100;
    private static final int FORBIDDEN = 40300;
    private static final int CONSUMER_GROUP_NOT_FOUND = 40403;
    private static final int TOO_MANY_REQUESTS = 42900;
    private static final int INTERNAL_ERROR = 50000;
    private static final int PROXY_TIMEOUT = 50400;
    private static final int UNSUPPORTED = 50500;

    private static final String REQUEST_ID = "request-id";

    @Test
    void ackBeforeStartThrowsTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        try {
            assertThatThrownBy(() -> reader.ack(handle()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not started yet");
        } finally {
            reader.close();
        }
    }

    @Test
    void ackAfterCloseThrowsTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final RecordingPopConsumer consumer = new RecordingPopConsumer();
        reader.setConsumer(consumer);
        reader.close();

        assertThatThrownBy(() -> reader.ack(handle()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already closed");
        assertThat(consumer.acked).isEmpty();
    }

    @Test
    void ackDelegatesRebuiltMessageViewToConsumerTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final RecordingPopConsumer consumer = new RecordingPopConsumer();
        reader.setConsumer(consumer);

        final RocketMQReceiptHandle handle = handle();
        try {
            reader.ack(handle);
        } finally {
            reader.close();
        }

        assertThat(consumer.acked).hasSize(1);
        final org.apache.rocketmq.client.java.message.MessageViewImpl acked =
                (org.apache.rocketmq.client.java.message.MessageViewImpl) consumer.acked.get(0);
        assertThat(acked.getMessageId())
                .isEqualTo(MessageIdCodec.getInstance().decode(handle.getMessageId()));
        assertThat(acked.getTopic()).isEqualTo(handle.getTopic());
        assertThat(acked.getReceiptHandle()).isEqualTo(handle.getReceiptHandle());
    }

    /**
     * A consumer group the server registered as LITE cannot be used by a SIMPLE mode source: the
     * proxy answers every request with {@code ILLEGAL_CONSUMER_GROUP}. The reader must fail the
     * task instead of retrying that rejection forever while the job stays RUNNING and reads
     * nothing.
     */
    @Test
    void liteGroupRejectedOnReceiveFailsFetchTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final ClientException rejection =
                new BadRequestException(
                        ILLEGAL_CONSUMER_GROUP,
                        REQUEST_ID,
                        "lite group must use LITE mode: GID-test");
        reader.startWorkersWith(new FailingPopConsumer(rejection, 1));
        try {
            assertThatThrownBy(() -> awaitFetchFailure(reader))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasRootCause(rejection)
                    .hasStackTraceContaining("lite group must use LITE mode: GID-test");
        } finally {
            reader.close();
        }
    }

    @Test
    void unexpectedReceiveFailureFailsFetchTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final IllegalStateException failure =
                new IllegalStateException("Stream is already completed, no further calls allowed");
        reader.startWorkersWith(new FailingPopConsumer(failure, 1));
        try {
            assertThatThrownBy(() -> awaitFetchFailure(reader))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasRootCause(failure);
        } finally {
            reader.close();
        }
    }

    @Test
    void transientReceiveFailureKeepsRetryingTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final FailingPopConsumer consumer =
                new FailingPopConsumer(
                        new InternalErrorException(INTERNAL_ERROR, REQUEST_ID, "internal error"),
                        2);
        reader.startWorkersWith(consumer);
        try {
            awaitReceiveAttempts(consumer, 3);
            // The reader keeps working: no failure is surfaced to the fetcher thread.
            assertThat(reader.fetch().nextSplit()).isNull();
            assertThat(consumer.attempts.get()).isGreaterThanOrEqualTo(3);
        } finally {
            reader.close();
        }
    }

    @Test
    void permanentFailureClassificationTest() {
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new BadRequestException(
                                        ILLEGAL_CONSUMER_GROUP, REQUEST_ID, "lite group")))
                .isTrue();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new UnauthorizedException(
                                        UNAUTHORIZED, REQUEST_ID, "no credential")))
                .isTrue();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new NotFoundException(
                                        CONSUMER_GROUP_NOT_FOUND, REQUEST_ID, "group not found")))
                .isTrue();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new UnsupportedException(UNSUPPORTED, REQUEST_ID, "unsupported")))
                .isTrue();

        // FORBIDDEN is also the proxy's backpressure answer while a consumer holds too many
        // unacknowledged messages, so it must stay retryable.
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new ForbiddenException(FORBIDDEN, REQUEST_ID, "too many unacked")))
                .isFalse();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new TooManyRequestsException(
                                        TOO_MANY_REQUESTS, REQUEST_ID, "throttled")))
                .isFalse();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new InternalErrorException(INTERNAL_ERROR, REQUEST_ID, "internal")))
                .isFalse();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new ProxyTimeoutException(PROXY_TIMEOUT, REQUEST_ID, "timeout")))
                .isFalse();
        assertThat(
                        RocketMQGrpcSourceSplitReader.isPermanentFailure(
                                new ClientException("unmapped failure")))
                .isFalse();
    }

    /**
     * Creating the consumer fails with an unchecked exception when the endpoints are missing. It
     * must fail the split assignment, and a later assignment must try again rather than silently
     * doing nothing because the reader still believes it is started.
     */
    @Test
    void consumerCreationFailureFailsSplitAssignmentTest() throws Exception {
        final RocketMQGrpcSourceSplitReader reader =
                new RocketMQGrpcSourceSplitReader(new Configuration());
        final SplitsChange<RocketMQGrpcSourceSplit> change =
                new SplitsAddition<>(Collections.singletonList(RocketMQGrpcSourceSplit.INSTANCE));
        try {
            assertThatThrownBy(() -> reader.handleSplitsChanges(change))
                    .isInstanceOf(FlinkRuntimeException.class)
                    .hasRootCauseInstanceOf(IllegalArgumentException.class)
                    .hasStackTraceContaining("endpoints");
            assertThatThrownBy(() -> reader.handleSplitsChanges(change))
                    .isInstanceOf(FlinkRuntimeException.class);
        } finally {
            reader.close();
        }
    }

    /**
     * Calls {@link RocketMQGrpcSourceSplitReader#fetch()} until it fails or the deadline passes.
     */
    private static void awaitFetchFailure(RocketMQGrpcSourceSplitReader reader) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            reader.fetch();
        }
        throw new AssertionError("fetch() did not fail within 30s");
    }

    private static void awaitReceiveAttempts(FailingPopConsumer consumer, int attempts)
            throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (consumer.attempts.get() < attempts && System.nanoTime() < deadline) {
            Thread.sleep(20L);
        }
    }

    private static RocketMQReceiptHandle handle() {
        return new RocketMQReceiptHandle(
                "127.0.0.1:8081",
                "ns-1",
                "GID-test",
                "normal-topic",
                null,
                MessageIdCodec.getInstance().nextMessageId().toString(),
                "receipt-handle-abc",
                1);
    }

    /**
     * A {@link PopConsumer} whose {@code receive()} fails the first {@code failures} calls with the
     * given throwable and returns no message afterwards.
     */
    private static final class FailingPopConsumer implements PopConsumer {

        private final Throwable failure;
        private final int failures;
        private final AtomicInteger attempts = new AtomicInteger();

        private FailingPopConsumer(Throwable failure, int failures) {
            this.failure = failure;
            this.failures = failures;
        }

        @Override
        public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
                throws ClientException {
            if (attempts.incrementAndGet() <= failures) {
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                throw (ClientException) failure;
            }
            return Collections.emptyList();
        }

        @Override
        public void ack(MessageView messageView) throws ClientException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
                throws ClientException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class RecordingPopConsumer implements PopConsumer {

        private final List<MessageView> acked = new ArrayList<>();

        @Override
        public List<MessageView> receive(int maxMessageNum, Duration invisibleDuration)
                throws ClientException {
            return Collections.emptyList();
        }

        @Override
        public void ack(MessageView messageView) throws ClientException {
            acked.add(messageView);
        }

        @Override
        public void changeInvisibleDuration(MessageView messageView, Duration invisibleDuration)
                throws ClientException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {}
    }
}
