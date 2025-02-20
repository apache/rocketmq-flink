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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.common.event.SourceCheckEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceDetectEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceInitAssignEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceReportOffsetEvent;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplitState;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    private final RocketMQSplitReader reader;

    public RocketMQSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageView>> elementsQueue,
            RocketMQSourceFetcherManager rocketmqSourceFetcherManager,
            RecordEmitter<MessageView, T, RocketMQSourceSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            RocketMQSourceReaderMetrics rocketMQSourceReaderMetrics,
            Supplier<SplitReader<MessageView, RocketMQSourceSplit>> readerSupplier) {

        super(elementsQueue, rocketmqSourceFetcherManager, recordEmitter, config, context);
        this.offsetsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
        this.commitOffsetsOnCheckpoint =
                config.get(RocketMQSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
        this.rocketmqSourceReaderMetrics = rocketMQSourceReaderMetrics;
        this.reader = (RocketMQSplitReader) readerSupplier.get();
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
        // Report checkpoint progress.
        SourceReportOffsetEvent sourceEvent = new SourceReportOffsetEvent();
        sourceEvent.setBroker(splitState.getBrokerName());
        sourceEvent.setTopic(splitState.getTopic());
        sourceEvent.setQueueId(splitState.getQueueId());
        sourceEvent.setCheckpoint(splitState.getCurrentOffset());
        context.sendSourceEventToCoordinator(sourceEvent);
        LOG.info("Report checkpoint progress: {}", sourceEvent);
        return splitState.getSourceSplit();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceDetectEvent) {
            handleSourceDetectEvent();
        } else if (sourceEvent instanceof SourceCheckEvent) {
            handleSourceCheckEvent((SourceCheckEvent) sourceEvent);
        }
    }

    // ------------------------
    private void handleSourceDetectEvent() {
        SourceInitAssignEvent sourceEvent1 = new SourceInitAssignEvent();
        List<RocketMQSourceSplit> splits = new LinkedList<>();
        ConcurrentMap<MessageQueue, Tuple2<Long, Long>> currentOffsetTable =
                reader.getCurrentOffsetTable();

        if (!currentOffsetTable.isEmpty()) {
            for (Map.Entry<MessageQueue, Tuple2<Long, Long>> entry :
                    currentOffsetTable.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                Long startOffset = entry.getValue().f0;
                Long stopOffset = entry.getValue().f1;
                RocketMQSourceSplit split =
                        new RocketMQSourceSplit(messageQueue, startOffset, stopOffset);
                splits.add(split);
            }
        }
        sourceEvent1.setSplits(splits);
        context.sendSourceEventToCoordinator(sourceEvent1);
        reader.setInitFinish(true);
    }

    private void handleSourceCheckEvent(SourceCheckEvent sourceEvent) {
        Map<MessageQueue, Tuple2<Long, Long>> checkMap = sourceEvent.getAssignedMq();
        Set<MessageQueue> assignedMq = checkMap.keySet();
        Set<MessageQueue> currentMq = reader.getCurrentOffsetTable().keySet();
        Set<MessageQueue> increaseSet = Sets.difference(assignedMq, currentMq);
        Set<MessageQueue> decreaseSet = Sets.difference(currentMq, assignedMq);

        if (increaseSet.isEmpty() && decreaseSet.isEmpty()) {
            LOG.info("No need to checkpoint, current assigned mq is same as before.");
        }

        if (!increaseSet.isEmpty()) {
            SplitsAddition<RocketMQSourceSplit> increase;
            increase =
                    new SplitsAddition<>(
                            increaseSet.stream()
                                    .map(
                                            mq ->
                                                    new RocketMQSourceSplit(
                                                            mq,
                                                            checkMap.get(mq).f0,
                                                            checkMap.get(mq).f1,
                                                            true))
                                    .collect(Collectors.toList()));
            reader.handleSplitsChanges(increase);
        }
        if (!decreaseSet.isEmpty()) {
            SplitsAddition<RocketMQSourceSplit> decrease;
            decrease =
                    new SplitsAddition<>(
                            decreaseSet.stream()
                                    .map(
                                            mq ->
                                                    new RocketMQSourceSplit(
                                                            mq,
                                                            checkMap.get(mq).f0,
                                                            checkMap.get(mq).f1,
                                                            false))
                                    .collect(Collectors.toList()));
            reader.handleSplitsChanges(decrease);
        }
    }

    @VisibleForTesting
    SortedMap<Long, Map<MessageQueue, Long>> getOffsetsToCommit() {
        return offsetsToCommit;
    }

    @VisibleForTesting
    int getNumAliveFetchers() {
        return splitFetcherManager.getNumAliveFetchers();
    }
}
