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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.InnerConsumerImpl;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link SplitReader} implementation that reads records from RocketMQ partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 */
@Internal
public class RocketMQSplitReader<T> implements SplitReader<MessageView, RocketMQSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSplitReader.class);

    private volatile boolean wakeup = false;

    private final InnerConsumer consumer;
    private final Configuration configuration;
    private final SourceReaderContext sourceReaderContext;
    private final RocketMQDeserializationSchema<T> deserializationSchema;

    // These maps need to be concurrent because it will be accessed by both the main thread
    // and the split fetcher thread in the callback.
    private final boolean commitOffsetsOnCheckpoint;
    private final SortedMap<Long, Map<MessageQueue, Long>> offsetsToCommit;

    private final ConcurrentMap<MessageQueue, Tuple2<Long, Long>> currentOffsetTable;
    private final RocketMQSourceReaderMetrics rocketmqSourceReaderMetrics;

    public RocketMQSplitReader(
            Configuration configuration,
            SourceReaderContext sourceReaderContext,
            RocketMQDeserializationSchema<T> deserializationSchema,
            RocketMQSourceReaderMetrics rocketmqSourceReaderMetrics) {

        this.configuration = configuration;
        this.sourceReaderContext = sourceReaderContext;
        this.deserializationSchema = deserializationSchema;
        this.offsetsToCommit = new TreeMap<>();
        this.currentOffsetTable = new ConcurrentHashMap<>();

        this.consumer = new InnerConsumerImpl(configuration);
        this.consumer.start();

        this.rocketmqSourceReaderMetrics = rocketmqSourceReaderMetrics;
        this.commitOffsetsOnCheckpoint =
                configuration.getBoolean(RocketMQOptions.COMMIT_OFFSETS_ON_CHECKPOINT);
    }

    @Override
    public RecordsWithSplitIds<MessageView> fetch() throws IOException {
        wakeup = false;
        RocketMQRecordsWithSplitIds<MessageView> recordsWithSplitIds =
                new RocketMQRecordsWithSplitIds<>(rocketmqSourceReaderMetrics);
        try {
            Duration duration =
                    Duration.ofMillis(this.configuration.getLong(RocketMQOptions.POLL_TIMEOUT));
            List<MessageView> messageExtList = consumer.poll(duration);
            for (MessageView messageView : messageExtList) {
                String splitId =
                        UtilAll.getSplitId(
                                new MessageQueue(
                                        messageView.getTopic(),
                                        messageView.getBrokerName(),
                                        messageView.getQueueId()));
                recordsWithSplitIds.recordsForSplit(splitId).add(messageView);
                if (this.configuration.getBoolean(RocketMQSourceOptions.GLOBAL_DEBUG_MODE)) {
                    LOG.info(
                            "Reader fetch splitId: {}, messageId: {}",
                            splitId,
                            messageView.getMessageId());
                }
            }
            recordsWithSplitIds.prepareForRead();
        } catch (Exception e) {
            LOG.error("Reader fetch split error", e);
        }
        return recordsWithSplitIds;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RocketMQSourceSplit> splitsChange) {
        // Current not support assign addition splits.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        ConcurrentMap<MessageQueue, Tuple2<Long, Long>> newOffsetTable = new ConcurrentHashMap<>();

        // Set up the stopping timestamps.
        splitsChange
                .splits()
                .forEach(
                        split -> {
                            MessageQueue messageQueue =
                                    new MessageQueue(
                                            split.getTopic(),
                                            split.getBrokerName(),
                                            split.getQueueId());
                            newOffsetTable.put(
                                    messageQueue,
                                    new Tuple2<>(
                                            split.getStartingOffset(), split.getStoppingOffset()));
                            rocketmqSourceReaderMetrics.registerNewMessageQueue(messageQueue);
                        });

        // todo: log message queue change

        // It will replace the previous assignment
        Set<MessageQueue> incrementalSplits = newOffsetTable.keySet();
        consumer.assign(incrementalSplits);

        // set offset to consumer
        for (Map.Entry<MessageQueue, Tuple2<Long, Long>> entry : newOffsetTable.entrySet()) {
            MessageQueue messageQueue = entry.getKey();
            Long startingOffset = entry.getValue().f0;
            try {
                consumer.seek(messageQueue, startingOffset);
            } catch (Exception e) {
                String info =
                        String.format(
                                "messageQueue:%s, seek to starting offset:%s",
                                messageQueue, startingOffset);
                throw new FlinkRuntimeException(info, e);
            }
        }
    }

    @Override
    public void wakeUp() {
        LOG.debug("Wake up the split reader in case the fetcher thread is blocking in fetch().");
        wakeup = true;
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (Exception e) {
            LOG.error("close consumer error", e);
        }
    }

    public void notifyCheckpointComplete(Map<MessageQueue, Long> offsetsToCommit) {
        if (offsetsToCommit != null) {
            for (Map.Entry<MessageQueue, Long> entry : offsetsToCommit.entrySet()) {
                consumer.commitOffset(entry.getKey(), entry.getValue());
            }
        }
    }

    private void finishSplitAtRecord(
            MessageQueue messageQueue,
            long currentOffset,
            RocketMQRecordsWithSplitIds<MessageView> recordsBySplits) {

        LOG.info("message queue {} has reached stopping offset {}", messageQueue, currentOffset);
        // recordsBySplits.addFinishedSplit(getSplitId(messageQueue));
        this.currentOffsetTable.remove(messageQueue);
    }

    // ---------------- private helper class ------------------------

    private static class RocketMQRecordsWithSplitIds<T> implements RecordsWithSplitIds<T> {

        // Mark split message queue identifier as current split id
        private String currentSplitId;

        private final Set<String> finishedSplits = new HashSet<>();
        private Iterator<T> recordIterator;

        private final Map<String, List<T>> recordsBySplits = new HashMap<>();
        private Iterator<Map.Entry<String, List<T>>> splitIterator;

        private final RocketMQSourceReaderMetrics readerMetrics;

        public RocketMQRecordsWithSplitIds(RocketMQSourceReaderMetrics readerMetrics) {
            this.readerMetrics = readerMetrics;
        }

        /** return records container */
        private Collection<T> recordsForSplit(String splitId) {
            return this.recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void addFinishedSplit(String splitId) {
            this.finishedSplits.add(splitId);
        }

        private void prepareForRead() {
            this.splitIterator = recordsBySplits.entrySet().iterator();
        }

        /**
         * Moves to the next split. This method is also called initially to move to the first split.
         * Returns null, if no splits are left.
         */
        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, List<T>> entry = splitIterator.next();
                currentSplitId = entry.getKey();
                recordIterator = entry.getValue().iterator();
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
                return null;
            }
        }

        /**
         * Gets the next record from the current split. Returns null if no more records are left in
         * this split.
         */
        @Nullable
        @Override
        public T nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                return recordIterator.next();
                // todo: support metrics here
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
