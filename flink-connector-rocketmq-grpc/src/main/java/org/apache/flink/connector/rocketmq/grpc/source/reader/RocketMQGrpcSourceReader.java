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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessage;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The source reader for the RocketMQ gRPC connector. It emits {@link AckableMessage} records and
 * its acknowledgement behaviour depends on the consumer mode:
 *
 * <ul>
 *   <li>LITE mode: the reader does not acknowledge messages itself; acknowledgement is performed by
 *       a downstream operator using the self-contained receipt handle carried by each emitted
 *       record.
 *   <li>SIMPLE mode: the reader acknowledges the emitted messages itself once the checkpoint that
 *       observed them completes, issuing the acks through the very {@link
 *       RocketMQGrpcSourceSplitReader} consumer that received them — no separate ack client and no
 *       second connection are involved. This requires checkpointing to be enabled and the invisible
 *       duration to be larger than the checkpoint interval plus the checkpoint timeout.
 * </ul>
 *
 * <p>In both modes un-acked messages are redelivered by the broker after their invisible duration
 * expires, providing the at-least-once guarantee.
 */
@Internal
public class RocketMQGrpcSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                MessageViewImpl,
                AckableMessage<T>,
                RocketMQGrpcSourceSplit,
                RocketMQGrpcSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQGrpcSourceReader.class);

    @Nullable private final CheckpointAckTracker ackTracker;

    /**
     * The split reader that received the messages; only consulted in SIMPLE mode, i.e. when {@link
     * #ackTracker} is non-null. It is not closed here: the fetcher manager owns its lifecycle and
     * shuts it down as part of the base reader close.
     */
    @Nullable private final RocketMQGrpcSourceSplitReader splitReader;

    public RocketMQGrpcSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageViewImpl>> elementsQueue,
            RocketMQGrpcSourceFetcherManager fetcherManager,
            RocketMQGrpcSourceRecordEmitter<T> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        this(elementsQueue, fetcherManager, recordEmitter, config, context, null, null);
    }

    public RocketMQGrpcSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageViewImpl>> elementsQueue,
            RocketMQGrpcSourceFetcherManager fetcherManager,
            RocketMQGrpcSourceRecordEmitter<T> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            @Nullable CheckpointAckTracker ackTracker,
            @Nullable RocketMQGrpcSourceSplitReader splitReader) {
        super(elementsQueue, fetcherManager, recordEmitter, config, context);
        this.ackTracker = ackTracker;
        this.splitReader = splitReader;
    }

    @Override
    public List<RocketMQGrpcSourceSplit> snapshotState(long checkpointId) {
        final List<RocketMQGrpcSourceSplit> splits = super.snapshotState(checkpointId);
        if (ackTracker != null) {
            ackTracker.snapshot(checkpointId);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (ackTracker == null || splitReader == null) {
            return;
        }
        for (RocketMQReceiptHandle handle : ackTracker.completeUpTo(checkpointId)) {
            try {
                splitReader.ack(handle);
            } catch (Exception e) {
                // The handle is already removed from the tracker, so tolerating the failure
                // redelivers only this one message once its invisible duration expires, while
                // failing the job would redeliver everything since the last checkpoint.
                LOG.warn(
                        "Failed to acknowledge message {} on checkpoint {} completion; "
                                + "the broker will redeliver the message.",
                        handle.getMessageId(),
                        checkpointId,
                        e);
            }
        }
    }

    @Override
    protected void onSplitFinished(Map<String, RocketMQGrpcSourceSplitState> finishedSplitIds) {
        // The Pop model is unbounded; splits do not finish.
    }

    @Override
    protected RocketMQGrpcSourceSplitState initializedState(RocketMQGrpcSourceSplit split) {
        return new RocketMQGrpcSourceSplitState(split);
    }

    @Override
    protected RocketMQGrpcSourceSplit toSplitType(
            String splitId, RocketMQGrpcSourceSplitState splitState) {
        return splitState.toRocketMQGrpcSourceSplit();
    }

    @VisibleForTesting
    int getNumAliveFetchers() {
        return splitFetcherManager.getNumAliveFetchers();
    }
}
