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

package org.apache.rocketmq.flink.legacy;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.legacy.common.config.OffsetResetStrategy;
import org.apache.rocketmq.flink.legacy.common.config.StartupMode;
import org.apache.rocketmq.flink.legacy.common.serialization.KeyValueDeserializationSchema;
import org.apache.rocketmq.flink.legacy.common.util.MetricUtils;
import org.apache.rocketmq.flink.legacy.common.util.RetryUtil;
import org.apache.rocketmq.flink.legacy.common.util.RocketMQUtils;
import org.apache.rocketmq.flink.legacy.common.watermark.WaterMarkForAll;
import org.apache.rocketmq.flink.legacy.common.watermark.WaterMarkPerQueue;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.com.google.common.collect.Lists;
import org.apache.flink.shaded.curator5.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_BATCH_SIZE;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_TIMEOUT;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.DEFAULT_CONSUMER_BATCH_SIZE;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.DEFAULT_CONSUMER_TIMEOUT;
import static org.apache.rocketmq.flink.legacy.common.util.RocketMQUtils.getInteger;

/**
 * The RocketMQSource is based on RocketMQ pull consumer mode, and provides exactly once reliability
 * guarantees when checkpoints are enabled. Otherwise, the source doesn't provide any reliability
 * guarantees.
 */
public class RocketMQSourceFunction<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceFunction.class);
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    private RunningChecker runningChecker;

    private transient DefaultLitePullConsumer consumer;

    private KeyValueDeserializationSchema<OUT> schema;
    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private Map<MessageQueue, Long> offsetTable;
    private Map<MessageQueue, Long> restoredOffsets;
    private List<MessageQueue> messageQueues;
    private ExecutorService executor;

    // watermark in source
    private WaterMarkPerQueue waterMarkPerQueue;
    private WaterMarkForAll waterMarkForAll;

    private ScheduledExecutorService timer;
    /** Data for pending but uncommitted offsets. */
    private LinkedMap pendingOffsetsToCommit;

    private Properties props;
    private String topic;
    private String group;
    private transient volatile boolean restored;
    private transient boolean enableCheckpoint;
    private volatile Object checkPointLock;

    private Meter tpsMetric;
    private MetricUtils.TimestampGauge fetchDelay = new MetricUtils.TimestampGauge();
    private MetricUtils.TimestampGauge emitDelay = new MetricUtils.TimestampGauge();

    /** The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
    private StartupMode startMode = StartupMode.GROUP_OFFSETS;

    /**
     * If StartupMode#GROUP_OFFSETS has no commit offset.OffsetResetStrategy would offer init
     * strategy.
     */
    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.LATEST;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    private Map<MessageQueue, Long> specificStartupOffsets;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.
     */
    private long specificTimeStamp;

    public RocketMQSourceFunction(KeyValueDeserializationSchema<OUT> schema, Properties props) {
        this.schema = schema;
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.debug("source open....");
        Validate.notEmpty(props, "Consumer properties can not be empty");

        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        Validate.notEmpty(topic, "Consumer topic can not be empty");
        Validate.notEmpty(group, "Consumer group can not be empty");

        String tag = props.getProperty(RocketMQConfig.CONSUMER_TAG);
        String sql = props.getProperty(RocketMQConfig.CONSUMER_SQL);
        Validate.isTrue(
                !(StringUtils.isNotEmpty(tag) && StringUtils.isNotEmpty(sql)),
                "Consumer tag and sql can not set value at the same time");

        this.enableCheckpoint =
                ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

        if (offsetTable == null) {
            offsetTable = new ConcurrentHashMap<>();
        }
        if (restoredOffsets == null) {
            restoredOffsets = new ConcurrentHashMap<>();
        }
        if (pendingOffsetsToCommit == null) {
            pendingOffsetsToCommit = new LinkedMap();
        }
        if (checkPointLock == null) {
            checkPointLock = new ReentrantLock();
        }
        if (waterMarkPerQueue == null) {
            waterMarkPerQueue = new WaterMarkPerQueue(5000);
        }
        if (waterMarkForAll == null) {
            waterMarkForAll = new WaterMarkForAll(5000);
        }
        if (timer == null) {
            timer = Executors.newSingleThreadScheduledExecutor();
        }

        runningChecker = new RunningChecker();
        runningChecker.setRunning(true);

        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("rmq-pull-thread-%d")
                        .build();
        executor = Executors.newCachedThreadPool(threadFactory);

        int indexOfThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
        consumer = new DefaultLitePullConsumer(group, RocketMQConfig.buildAclRPCHook(props));
        RocketMQConfig.buildConsumerConfigs(props, consumer);

        // set unique instance name, avoid exception:
        // https://help.aliyun.com/document_detail/29646.html
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        String instanceName =
                RocketMQUtils.getInstanceName(
                        runtimeName,
                        topic,
                        group,
                        String.valueOf(indexOfThisSubTask),
                        String.valueOf(System.nanoTime()));
        consumer.setInstanceName(instanceName);
        consumer.start();

        Counter outputCounter =
                getRuntimeContext()
                        .getMetricGroup()
                        .counter(MetricUtils.METRICS_TPS + "_counter", new SimpleCounter());
        tpsMetric =
                getRuntimeContext()
                        .getMetricGroup()
                        .meter(MetricUtils.METRICS_TPS, new MeterView(outputCounter, 60));

        getRuntimeContext()
                .getMetricGroup()
                .gauge(MetricUtils.CURRENT_FETCH_EVENT_TIME_LAG, fetchDelay);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(MetricUtils.CURRENT_EMIT_EVENT_TIME_LAG, emitDelay);

        final RuntimeContext ctx = getRuntimeContext();
        // The lock that guarantees that record emission and state updates are atomic,
        // from the view of taking a checkpoint.
        int taskNumber = ctx.getNumberOfParallelSubtasks();
        int taskIndex = ctx.getIndexOfThisSubtask();
        log.info("Source run, NumberOfTotalTask={}, IndexOfThisSubTask={}", taskNumber, taskIndex);
        Collection<MessageQueue> totalQueues = consumer.fetchMessageQueues(topic);
        messageQueues =
                RocketMQUtils.allocate(totalQueues, taskNumber, ctx.getIndexOfThisSubtask());
        // If the job recovers from the state, the state has already contained the offsets of last
        // commit.
        if (restored) {
            initOffsetTableFromRestoredOffsets(messageQueues);
        } else {
            initOffsets(messageQueues);
        }
    }

    @Override
    public void run(SourceContext context) throws Exception {
        String sql = props.getProperty(RocketMQConfig.CONSUMER_SQL);
        String tag =
                props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);
        int pullBatchSize = getInteger(props, CONSUMER_BATCH_SIZE, DEFAULT_CONSUMER_BATCH_SIZE);
        timer.scheduleAtFixedRate(
                () -> {
                    // context.emitWatermark(waterMarkPerQueue.getCurrentWatermark());
                    context.emitWatermark(waterMarkForAll.getCurrentWatermark());
                },
                5,
                5,
                TimeUnit.SECONDS);
        if (StringUtils.isEmpty(sql)) {
            consumer.subscribe(topic, tag);
        } else {
            // pull with sql do not support block pull.
            consumer.subscribe(topic, MessageSelector.bySql(sql));
        }
        for (MessageQueue mq : messageQueues) {
            this.executor.execute(
                    () ->
                            RetryUtil.call(
                                    () -> {
                                        while (runningChecker.isRunning()) {
                                            try {
                                                Long offset = offsetTable.get(mq);
                                                consumer.setPullBatchSize(pullBatchSize);
                                                consumer.seek(mq, offset);
                                                boolean found = false;
                                                List<MessageExt> messages =
                                                        consumer.poll(
                                                                getInteger(
                                                                        props,
                                                                        CONSUMER_TIMEOUT,
                                                                        DEFAULT_CONSUMER_TIMEOUT));
                                                if (CollectionUtils.isNotEmpty(messages)) {
                                                    long fetchTime = System.currentTimeMillis();
                                                    for (MessageExt msg : messages) {
                                                        byte[] key =
                                                                msg.getKeys() != null
                                                                        ? msg.getKeys()
                                                                                .getBytes(
                                                                                        StandardCharsets
                                                                                                .UTF_8)
                                                                        : null;
                                                        byte[] value = msg.getBody();
                                                        OUT data =
                                                                schema.deserializeKeyAndValue(
                                                                        key, value);

                                                        // output and state update are atomic
                                                        synchronized (checkPointLock) {
                                                            log.debug(
                                                                    msg.getMsgId()
                                                                            + "_"
                                                                            + msg.getBrokerName()
                                                                            + " "
                                                                            + msg.getQueueId()
                                                                            + " "
                                                                            + msg.getQueueOffset());
                                                            context.collectWithTimestamp(
                                                                    data, msg.getBornTimestamp());
                                                            long emitTime =
                                                                    System.currentTimeMillis();
                                                            // update max eventTime per queue
                                                            // waterMarkPerQueue.extractTimestamp(mq, msg.getBornTimestamp());
                                                            waterMarkForAll.extractTimestamp(
                                                                    msg.getBornTimestamp());
                                                            tpsMetric.markEvent();
                                                            long eventTime =
                                                                    msg.getStoreTimestamp();
                                                            fetchDelay.report(
                                                                    Math.abs(
                                                                            fetchTime - eventTime));
                                                            emitDelay.report(
                                                                    Math.abs(emitTime - eventTime));
                                                        }
                                                    }
                                                    found = true;
                                                }
                                                synchronized (checkPointLock) {
                                                    updateMessageQueueOffset(
                                                            mq, consumer.committed(mq));
                                                }

                                                if (!found) {
                                                    RetryUtil.waitForMs(
                                                            RocketMQConfig
                                                                    .DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);
                                                }
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                        return true;
                                    },
                                    "RuntimeException",
                                    runningChecker));
        }

        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    /**
     * only flink job start with no state can init offsets from broker
     *
     * @param messageQueues
     * @throws MQClientException
     */
    private void initOffsets(List<MessageQueue> messageQueues) throws MQClientException {
        for (MessageQueue mq : messageQueues) {
            long offset;
            switch (startMode) {
                case LATEST:
                    consumer.seekToEnd(mq);
                    offset = consumer.committed(mq);
                    break;
                case EARLIEST:
                    consumer.seekToBegin(mq);
                    offset = consumer.committed(mq);
                    break;
                case GROUP_OFFSETS:
                    offset = consumer.committed(mq);
                    // the min offset return if consumer group first join,return a negative number
                    // if
                    // catch exception when fetch from broker.
                    // If you want consumer from earliest,please use OffsetResetStrategy.EARLIEST
                    if (offset <= 0) {
                        switch (offsetResetStrategy) {
                            case LATEST:
                                consumer.seekToEnd(mq);
                                offset = consumer.committed(mq);
                                log.info(
                                        "current consumer thread:{} has no committed offset,use Strategy:{} instead",
                                        mq,
                                        offsetResetStrategy);
                                break;
                            case EARLIEST:
                                log.info(
                                        "current consumer thread:{} has no committed offset,use Strategy:{} instead",
                                        mq,
                                        offsetResetStrategy);
                                consumer.seekToBegin(mq);
                                offset = consumer.committed(mq);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                case TIMESTAMP:
                    offset = consumer.offsetForTimestamp(mq, specificTimeStamp);
                    break;
                case SPECIFIC_OFFSETS:
                    if (specificStartupOffsets == null) {
                        throw new RuntimeException(
                                "StartMode is specific_offsets.But none offsets has been specified");
                    }
                    Long specificOffset = specificStartupOffsets.get(mq);
                    if (specificOffset != null) {
                        offset = specificOffset;
                    } else {
                        offset = consumer.committed(mq);
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "current startMode is not supported" + startMode);
            }
            log.info(
                    "current consumer queue:{} start from offset of: {}",
                    mq.getBrokerName() + "-" + mq.getQueueId(),
                    offset);
            offsetTable.put(mq, offset);
        }
    }

    /** consume from the min offset at every restart with no state */
    public RocketMQSourceFunction<OUT> setStartFromEarliest() {
        this.startMode = StartupMode.EARLIEST;
        return this;
    }

    /** consume from the max offset of each broker's queue at every restart with no state */
    public RocketMQSourceFunction<OUT> setStartFromLatest() {
        this.startMode = StartupMode.LATEST;
        return this;
    }

    /** consume from the closest offset */
    public RocketMQSourceFunction<OUT> setStartFromTimeStamp(long timeStamp) {
        this.startMode = StartupMode.TIMESTAMP;
        this.specificTimeStamp = timeStamp;
        return this;
    }

    /** consume from the group offsets those was stored in brokers. */
    public RocketMQSourceFunction<OUT> setStartFromGroupOffsets() {
        this.startMode = StartupMode.GROUP_OFFSETS;
        return this;
    }

    /**
     * consume from the group offsets those was stored in brokers. If there is no committed
     * offset,#{@link OffsetResetStrategy} would provide initialization policy.
     */
    public RocketMQSourceFunction<OUT> setStartFromGroupOffsets(
            OffsetResetStrategy offsetResetStrategy) {
        this.startMode = StartupMode.GROUP_OFFSETS;
        this.offsetResetStrategy = offsetResetStrategy;
        return this;
    }

    /**
     * consume from the specific offset. Group offsets is enable while the broker didn't specify
     * offset.
     */
    public RocketMQSourceFunction<OUT> setStartFromSpecificOffsets(
            Map<MessageQueue, Long> specificOffsets) {
        this.specificStartupOffsets = specificOffsets;
        this.startMode = StartupMode.SPECIFIC_OFFSETS;
        return this;
    }

    private void updateMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
        offsetTable.put(mq, offset);
        if (!enableCheckpoint) {
            consumer.getOffsetStore().updateOffset(mq, offset, false);
        }
    }

    @Override
    public void cancel() {
        log.debug("cancel ...");
        runningChecker.setRunning(false);

        if (timer != null) {
            timer.shutdown();
            timer = null;
        }

        if (executor != null) {
            executor.shutdown();
            executor = null;
        }

        if (consumer != null) {
            consumer.shutdown();
            consumer = null;
        }

        if (offsetTable != null) {
            offsetTable.clear();
            offsetTable = null;
        }
        if (restoredOffsets != null) {
            restoredOffsets.clear();
            restoredOffsets = null;
        }
        if (pendingOffsetsToCommit != null) {
            pendingOffsetsToCommit.clear();
            pendingOffsetsToCommit = null;
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("close ...");
        // pretty much the same logic as cancelling
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    public void initOffsetTableFromRestoredOffsets(List<MessageQueue> messageQueues) {
        Preconditions.checkNotNull(restoredOffsets, "restoredOffsets can't be null");
        restoredOffsets.forEach(
                (mq, offset) -> {
                    if (messageQueues.contains(mq)) {
                        offsetTable.put(mq, offset);
                    }
                });
        log.info("init offset table [{}] from restoredOffsets successful.", offsetTable);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested
        log.info("Snapshotting state {} ...", context.getCheckpointId());
        if (!runningChecker.isRunning()) {
            log.info("snapshotState() called on closed source; returning null.");
            return;
        }

        Map<MessageQueue, Long> currentOffsets;
        try {
            // Discovers topic route change when snapshot
            RetryUtil.call(
                    () -> {
                        Collection<MessageQueue> totalQueues = consumer.fetchMessageQueues(topic);
                        int taskNumber = getRuntimeContext().getNumberOfParallelSubtasks();
                        int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
                        List<MessageQueue> newQueues =
                                RocketMQUtils.allocate(totalQueues, taskNumber, taskIndex);
                        Collections.sort(newQueues);
                        log.debug(taskIndex + " Topic route is same.");
                        if (!messageQueues.equals(newQueues)) {
                            throw new RuntimeException();
                        }
                        return true;
                    },
                    "RuntimeException due to topic route changed");

            unionOffsetStates.clear();
            currentOffsets = new HashMap<>(offsetTable.size());
        } catch (RuntimeException e) {
            log.warn("Retry failed multiple times for topic route change, keep previous offset.");
            // If the retry fails for multiple times, the message queue and its offset in the
            // previous checkpoint will be retained.
            List<Tuple2<MessageQueue, Long>> unionOffsets =
                    Lists.newArrayList(unionOffsetStates.get().iterator());
            Map<MessageQueue, Long> queueOffsets = new HashMap<>(unionOffsets.size());
            unionOffsets.forEach(queueOffset -> queueOffsets.put(queueOffset.f0, queueOffset.f1));
            currentOffsets = new HashMap<>(unionOffsets.size() + offsetTable.size());
            currentOffsets.putAll(queueOffsets);
        }

        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
            currentOffsets.put(entry.getKey(), entry.getValue());
        }
        pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
        log.info(
                "Snapshot state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
                offsetTable,
                context.getCheckpointId(),
                context.getCheckpointTimestamp());
    }

    /**
     * called every time the user-defined function is initialized, be that when the function is
     * first initialized or be that when the function is actually recovering from an earlier
     * checkpoint. Given this, initializeState() is not only the place where different types of
     * state are initialized, but also where state recovery logic is included.
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info("initialize State ...");

        this.unionOffsetStates =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        OFFSETS_STATE_NAME,
                                        TypeInformation.of(
                                                new TypeHint<Tuple2<MessageQueue, Long>>() {})));
        this.restored = context.isRestored();

        if (restored) {
            if (restoredOffsets == null) {
                restoredOffsets = new ConcurrentHashMap<>();
            }
            for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
                if (!restoredOffsets.containsKey(mqOffsets.f0)
                        || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
                    restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
                }
            }
            log.info(
                    "Setting restore state in the consumer. Using the following offsets: {}",
                    restoredOffsets);
        } else {
            log.info("No restore state for the consumer.");
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // callback when checkpoint complete
        if (!runningChecker.isRunning()) {
            log.info("notifyCheckpointComplete() called on closed source; returning null.");
            return;
        }

        final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
        if (posInMap == -1) {
            log.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
            return;
        }

        Map<MessageQueue, Long> offsets =
                (Map<MessageQueue, Long>) pendingOffsetsToCommit.remove(posInMap);

        // remove older checkpoints in map
        for (int i = 0; i < posInMap; i++) {
            pendingOffsetsToCommit.remove(0);
        }

        if (offsets == null || offsets.size() == 0) {
            log.debug("Checkpoint state was empty.");
            return;
        }

        for (Map.Entry<MessageQueue, Long> entry : offsets.entrySet()) {
            consumer.getOffsetStore().updateOffset(entry.getKey(), entry.getValue(), false);
            consumer.getOffsetStore().persist(consumer.queueWithNamespace(entry.getKey()));
        }
    }
}
