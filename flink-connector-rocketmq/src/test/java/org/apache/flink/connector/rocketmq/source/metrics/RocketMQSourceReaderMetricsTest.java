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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RocketMQSourceReaderMetrics}. The metric groups are replaced by a recording fake
 * so the gauges and counters can be read back the way a metric reporter would.
 */
class RocketMQSourceReaderMetricsTest {

    private static final MessageQueue QUEUE_0 = new MessageQueue("TopicA", "broker-a", 0);
    private static final MessageQueue QUEUE_1 = new MessageQueue("TopicA", "broker-a", 1);

    private final RecordingSourceReaderMetricGroup metricGroup =
            new RecordingSourceReaderMetricGroup();
    private final RocketMQSourceReaderMetrics metrics =
            new RocketMQSourceReaderMetrics(metricGroup);

    @Test
    void registeredQueueExposesOffsetGaugesTest() {
        metrics.registerNewMessageQueue(QUEUE_0);
        metrics.setCurrentOffset(QUEUE_0, 42L);
        metrics.setCommittedOffset(QUEUE_0, 40L);

        RecordingMetricGroup queueGroup = queueGroup("TopicA", "0");
        assertThat(gaugeValue(queueGroup, "currentOffset")).isEqualTo(42L);
        assertThat(gaugeValue(queueGroup, "committedOffset")).isEqualTo(40L);
    }

    @Test
    void lagIsDerivedFromTheBrokerMaxOffsetTest() {
        metrics.registerNewMessageQueue(QUEUE_0);
        metrics.setCurrentOffset(QUEUE_0, 100L);
        metrics.updateMaxOffset(QUEUE_0, 250L);

        assertThat(gaugeValue(queueGroup("TopicA", "0"), "records-lag")).isEqualTo(150L);
        assertThat(metricGroup.pendingRecords.getValue()).isEqualTo(150L);
    }

    @Test
    void lagNeverGoesNegativeAndStartsAtZeroTest() {
        metrics.registerNewMessageQueue(QUEUE_0);

        assertThat(metricGroup.pendingRecords.getValue()).isZero();

        metrics.setCurrentOffset(QUEUE_0, 300L);
        metrics.updateMaxOffset(QUEUE_0, 120L);

        assertThat(metricGroup.pendingRecords.getValue()).isZero();
    }

    @Test
    void pendingRecordsSumsEveryAssignedQueueTest() {
        metrics.registerNewMessageQueue(QUEUE_0);
        metrics.registerNewMessageQueue(QUEUE_1);
        metrics.setCurrentOffset(QUEUE_0, 10L);
        metrics.updateMaxOffset(QUEUE_0, 30L);
        metrics.setCurrentOffset(QUEUE_1, 5L);
        metrics.updateMaxOffset(QUEUE_1, 100L);

        assertThat(metricGroup.pendingRecords.getValue()).isEqualTo(115L);
    }

    @Test
    void unregisteredQueueStopsContributingToLagTest() {
        metrics.registerNewMessageQueue(QUEUE_0);
        metrics.registerNewMessageQueue(QUEUE_1);
        metrics.setCurrentOffset(QUEUE_0, 10L);
        metrics.updateMaxOffset(QUEUE_0, 30L);
        metrics.setCurrentOffset(QUEUE_1, 5L);
        metrics.updateMaxOffset(QUEUE_1, 100L);

        metrics.unregisterMessageQueue(QUEUE_1);

        assertThat(metricGroup.pendingRecords.getValue()).isEqualTo(20L);
        // Updates for a queue that is no longer assigned must not resurrect it.
        metrics.updateMaxOffset(QUEUE_1, 500L);
        assertThat(metricGroup.pendingRecords.getValue()).isEqualTo(20L);
    }

    @Test
    void commitCountersTrackSuccessAndFailureTest() {
        metrics.recordCommitSucceeded();
        metrics.recordCommitSucceeded();
        metrics.recordCommitFailed();

        RecordingMetricGroup readerGroup = metricGroup.group("RocketmqSourceReader");
        assertThat(readerGroup.counters.get("commitsSucceeded").getCount()).isEqualTo(2L);
        assertThat(readerGroup.counters.get("commitsFailed").getCount()).isEqualTo(1L);
    }

    @Test
    void bytesConsumedCounterAccumulatesTest() {
        metrics.recordBytesConsumed(1024L);
        metrics.recordBytesConsumed(512L);

        RecordingMetricGroup fetchManager =
                metricGroup.group("RocketMQConsumer").group("consumer-fetch-manager-metrics");
        assertThat(fetchManager.counters.get("bytes-consumed-total").getCount()).isEqualTo(1536L);
    }

    private RecordingMetricGroup queueGroup(String topic, String queueId) {
        return metricGroup
                .group("RocketmqSourceReader")
                .group("topic=" + topic)
                .group("queue=" + queueId);
    }

    private static long gaugeValue(RecordingMetricGroup group, String name) {
        Gauge<?> gauge = group.gauges.get(name);
        assertThat(gauge).as("gauge %s", name).isNotNull();
        return ((Number) gauge.getValue()).longValue();
    }

    /** Records every metric registered on it so tests can read the values back. */
    private static class RecordingMetricGroup extends UnregisteredMetricsGroup {

        final Map<String, Gauge<?>> gauges = new HashMap<>();
        final Map<String, Counter> counters = new HashMap<>();
        final Map<String, RecordingMetricGroup> groups = new HashMap<>();

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(name, gauge);
            return gauge;
        }

        @Override
        public Counter counter(String name) {
            SimpleCounter counter = new SimpleCounter();
            counters.put(name, counter);
            return counter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return groups.computeIfAbsent(name, ignored -> new RecordingMetricGroup());
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return groups.computeIfAbsent(key + "=" + value, ignored -> new RecordingMetricGroup());
        }

        RecordingMetricGroup group(String name) {
            RecordingMetricGroup group = groups.get(name);
            assertThat(group).as("metric group %s", name).isNotNull();
            return group;
        }
    }

    /** The recording group plus the handful of methods only a source reader group declares. */
    private static class RecordingSourceReaderMetricGroup extends RecordingMetricGroup
            implements SourceReaderMetricGroup {

        Gauge<Long> pendingRecords;

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            return UnregisteredMetricsGroup.createOperatorIOMetricGroup();
        }

        @Override
        public Counter getNumRecordsInErrorsCounter() {
            return new SimpleCounter();
        }

        @Override
        public void setPendingBytesGauge(Gauge<Long> pendingBytesGauge) {}

        @Override
        public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {
            this.pendingRecords = pendingRecordsGauge;
        }
    }
}
