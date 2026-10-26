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

package org.apache.flink.connector.rocketmq.source.metrics;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics of the RocketMQ source reader.
 *
 * <p>Offsets are exposed per message queue under the {@code RocketmqSourceReader} group, keyed by
 * topic and queue id, so a reader that owns several queues can be inspected queue by queue. The lag
 * of every queue is additionally summed into {@code pendingRecords}, which is what Flink's
 * backpressure and idleness tooling reads.
 *
 * <p>Lag needs the maximum offset of a queue, which only the broker knows. The reader therefore
 * pushes the values in with {@link #updateMaxOffset} whenever it refreshes them, and the gauges
 * here only ever report the last known value; reading a metric never triggers a remote call.
 */
@PublicEvolving
public class RocketMQSourceReaderMetrics {

    public static final String ROCKETMQ_SOURCE_READER_METRIC_GROUP = "RocketmqSourceReader";
    public static final String TOPIC_GROUP = "topic";
    public static final String QUEUE_GROUP = "queue";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String COMMITTED_OFFSET_METRIC_GAUGE = "committedOffset";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";
    public static final String ROCKETMQ_CONSUMER_METRIC_GROUP = "RocketMQConsumer";

    public static final String CONSUMER_FETCH_MANAGER_GROUP = "consumer-fetch-manager-metrics";
    public static final String BYTES_CONSUMED_TOTAL = "bytes-consumed-total";
    public static final String RECORDS_LAG = "records-lag";

    private final MetricGroup readerGroup;
    private final Counter commitsSucceeded;
    private final Counter commitsFailed;
    private final Counter bytesConsumedTotal;
    private final Map<MessageQueue, QueueMetrics> queues = new ConcurrentHashMap<>();

    public RocketMQSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.readerGroup = sourceReaderMetricGroup.addGroup(ROCKETMQ_SOURCE_READER_METRIC_GROUP);
        this.commitsSucceeded = readerGroup.counter(COMMITS_SUCCEEDED_METRIC_COUNTER);
        this.commitsFailed = readerGroup.counter(COMMITS_FAILED_METRIC_COUNTER);

        MetricGroup fetchManagerGroup =
                sourceReaderMetricGroup
                        .addGroup(ROCKETMQ_CONSUMER_METRIC_GROUP)
                        .addGroup(CONSUMER_FETCH_MANAGER_GROUP);
        this.bytesConsumedTotal = fetchManagerGroup.counter(BYTES_CONSUMED_TOTAL);
        fetchManagerGroup.gauge(RECORDS_LAG, (Gauge<Long>) this::getTotalLag);

        sourceReaderMetricGroup.setPendingRecordsGauge((Gauge<Long>) this::getTotalLag);
    }

    /** Starts reporting the offsets of a newly assigned queue. */
    public void registerNewMessageQueue(MessageQueue messageQueue) {
        queues.computeIfAbsent(messageQueue, this::createQueueMetrics);
    }

    /**
     * Stops aggregating a queue that is no longer assigned to this reader.
     *
     * <p>{@link MetricGroup} offers no way to deregister from a connector, so the gauges of the
     * queue stay visible and keep reporting their last known values. The queue count of a topic is
     * finite, which bounds how many groups a reader can accumulate.
     */
    public void unregisterMessageQueue(MessageQueue messageQueue) {
        queues.remove(messageQueue);
    }

    /** Records how far the reader has consumed a queue. */
    public void setCurrentOffset(MessageQueue messageQueue, long offset) {
        QueueMetrics metrics = queues.get(messageQueue);
        if (metrics != null) {
            metrics.currentOffset.set(offset);
        }
    }

    /** Records the offset that was successfully committed to the broker for a queue. */
    public void setCommittedOffset(MessageQueue messageQueue, long offset) {
        QueueMetrics metrics = queues.get(messageQueue);
        if (metrics != null) {
            metrics.committedOffset.set(offset);
        }
    }

    /**
     * Records the maximum offset the broker holds for a queue and derives its lag from the offset
     * the reader has consumed so far.
     */
    public void updateMaxOffset(MessageQueue messageQueue, long maxOffset) {
        QueueMetrics metrics = queues.get(messageQueue);
        if (metrics != null) {
            long currentOffset = metrics.currentOffset.get();
            metrics.lag.set(currentOffset < 0L ? 0L : Math.max(0L, maxOffset - currentOffset));
        }
    }

    public void recordBytesConsumed(long bytes) {
        bytesConsumedTotal.inc(bytes);
    }

    public void recordCommitSucceeded() {
        commitsSucceeded.inc();
    }

    public void recordCommitFailed() {
        commitsFailed.inc();
    }

    private QueueMetrics createQueueMetrics(MessageQueue messageQueue) {
        MetricGroup queueGroup =
                readerGroup
                        .addGroup(TOPIC_GROUP, messageQueue.getTopic())
                        .addGroup(QUEUE_GROUP, String.valueOf(messageQueue.getQueueId()));
        QueueMetrics metrics = new QueueMetrics();
        queueGroup.gauge(CURRENT_OFFSET_METRIC_GAUGE, (Gauge<Long>) metrics.currentOffset::get);
        queueGroup.gauge(COMMITTED_OFFSET_METRIC_GAUGE, (Gauge<Long>) metrics.committedOffset::get);
        queueGroup.gauge(RECORDS_LAG, (Gauge<Long>) metrics.lag::get);
        return metrics;
    }

    private long getTotalLag() {
        long total = 0L;
        for (QueueMetrics metrics : queues.values()) {
            total += metrics.lag.get();
        }
        return total;
    }

    /** The mutable state behind the gauges of a single queue. */
    private static final class QueueMetrics {

        private final AtomicLong currentOffset = new AtomicLong(-1L);
        private final AtomicLong committedOffset = new AtomicLong(-1L);
        private final AtomicLong lag = new AtomicLong(0L);
    }
}
