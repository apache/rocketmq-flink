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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplitState;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;

import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** The source reader for RocketMQ partitions. */
public class RocketMQSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                MessageView, T, RocketMQSourceSplit, RocketMQSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceReader.class);

    // These maps need to be concurrent because it will be accessed by both the main thread
    // and the split fetcher thread in the callback.
    private final boolean commitOffsetsOnCheckpoint;
    private final SortedMap<Long, Map<MessageQueue, Long>> offsetsToCommit;
    private final ConcurrentMap<MessageQueue, Long> offsetsOfFinishedSplits;
    private final RocketMQSourceReaderMetrics rocketmqSourceReaderMetrics;

    public RocketMQSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageView>> elementsQueue,
            RocketMQSourceFetcherManager rocketmqSourceFetcherManager,
            RecordEmitter<MessageView, T, RocketMQSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            RocketMQSourceReaderMetrics rocketMQSourceReaderMetrics) {

        super(elementsQueue, rocketmqSourceFetcherManager, recordEmitter, config, context);
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
        this.commitOffsetsOnCheckpoint =
                config.get(RocketMQSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
        this.rocketmqSourceReaderMetrics = rocketMQSourceReaderMetrics;
    }

    @Override
    protected void onSplitFinished(Map<String, RocketMQSourceSplitState> finishedSplitIds) {
        finishedSplitIds.forEach(
                (ignored, splitState) -> {
                    if (splitState.getCurrentOffset() >= 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getMessageQueue(), splitState.getCurrentOffset());
                    }
                });
    }

    @Override
    public List<RocketMQSourceSplit> snapshotState(long checkpointId) {
        List<RocketMQSourceSplit> splits = super.snapshotState(checkpointId);
        if (!commitOffsetsOnCheckpoint) {
            return splits;
        }

        if (splits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            offsetsToCommit.put(checkpointId, Collections.emptyMap());
        } else {
            Map<MessageQueue, Long> offsetsMap =
                    offsetsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
            // Put the offsets of the active splits.
            for (RocketMQSourceSplit split : splits) {
                // If the checkpoint is triggered before the queue min offsets
                // is retrieved, do not commit the offsets for those partitions.
                if (split.getStartingOffset() >= 0) {
                    offsetsMap.put(UtilAll.getMessageQueue(split), split.getStartingOffset());
                }
            }
            // Put offsets of all the finished splits.
            offsetsMap.putAll(offsetsOfFinishedSplits);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.debug("Committing offsets for checkpoint {}", checkpointId);
        if (!commitOffsetsOnCheckpoint) {
            return;
        }

        Map<MessageQueue, Long> committedPartitions = offsetsToCommit.get(checkpointId);
        if (committedPartitions == null) {
            LOG.info(
                    "Offsets for checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }

        ((RocketMQSourceFetcherManager) splitFetcherManager).commitOffsets(committedPartitions);
    }

    @Override
    protected RocketMQSourceSplitState initializedState(RocketMQSourceSplit partitionSplit) {
        return new RocketMQSourceSplitState(partitionSplit);
    }

    @Override
    protected RocketMQSourceSplit toSplitType(String splitId, RocketMQSourceSplitState splitState) {
        return splitState.getSourceSplit();
    }

    // ------------------------

    @VisibleForTesting
    SortedMap<Long, Map<MessageQueue, Long>> getOffsetsToCommit() {
        return offsetsToCommit;
    }

    @VisibleForTesting
    int getNumAliveFetchers() {
        return splitFetcherManager.getNumAliveFetchers();
    }
}
