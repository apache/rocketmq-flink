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

package org.apache.flink.connector.rocketmq.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.event.SourceCheckEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceDetectEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceInitAssignEvent;
import org.apache.flink.connector.rocketmq.common.event.SourceReportOffsetEvent;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.InnerConsumerImpl;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategyFactory;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The enumerator class for RocketMQ source. */
@Internal
public class RocketMQSourceEnumerator
        implements SplitEnumerator<RocketMQSourceSplit, RocketMQSourceEnumState> {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceEnumerator.class);

    private final Configuration configuration;
    private final SplitEnumeratorContext<RocketMQSourceSplit> context;
    private final Boundedness boundedness;

    private InnerConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private final AllocateStrategy allocateStrategy;
    private final OffsetsSelector startingOffsetsSelector;
    private final OffsetsSelector stoppingOffsetsSelector;

    // Used for queue dynamic allocation
    private final Map<MessageQueue, Long> checkedOffsets;
    private boolean[] initTask;

    // The internal states of the enumerator.
    // This set is only accessed by the partition discovery callable in the callAsync() method.
    // The current assignment by reader id. Only accessed by the coordinator thread.
    // The discovered and initialized partition splits that are waiting for owner reader to be
    // ready.
    private final Set<MessageQueue> allocatedSet;
    private final Map<Integer, Set<RocketMQSourceSplit>> pendingSplitAssignmentMap;
    private Map<Integer, Set<RocketMQSourceSplit>> assignedMap;
    private final Map<MessageQueue, Integer /* taskId */> reflectedQueueToTaskId;

    // Param from configuration
    private final String groupId;
    private final long partitionDiscoveryIntervalMs;

    // Indicates the number of allocated queues
    private int partitionId;
    private ReentrantLock lock;

    public RocketMQSourceEnumerator(
            OffsetsSelector startingOffsetsSelector,
            OffsetsSelector stoppingOffsetsSelector,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQSourceSplit> context) {

        this(
                startingOffsetsSelector,
                stoppingOffsetsSelector,
                boundedness,
                configuration,
                context,
                new HashSet<>());
    }

    public RocketMQSourceEnumerator(
            OffsetsSelector startingOffsetsSelector,
            OffsetsSelector stoppingOffsetsSelector,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQSourceSplit> context,
            Set<MessageQueue> currentSplitAssignment) {
        this.configuration = configuration;
        this.context = context;
        this.boundedness = boundedness;
        this.lock = new ReentrantLock();

        // Support allocate splits to reader
        this.checkedOffsets = new ConcurrentHashMap<>();
        this.reflectedQueueToTaskId = new ConcurrentHashMap<>();
        this.pendingSplitAssignmentMap = new ConcurrentHashMap<>();
        this.allocatedSet = new HashSet<>(currentSplitAssignment);
        this.assignedMap = new ConcurrentHashMap<>();
        this.allocateStrategy =
                AllocateStrategyFactory.getStrategy(
                        configuration, context, new RocketMQSourceEnumState(allocatedSet));

        // For rocketmq setting
        this.groupId = configuration.getString(RocketMQSourceOptions.CONSUMER_GROUP);
        this.startingOffsetsSelector = startingOffsetsSelector;
        this.stoppingOffsetsSelector = stoppingOffsetsSelector;
        this.partitionDiscoveryIntervalMs =
                configuration.getLong(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);

        // Initialize the task status
        if (!allocatedSet.isEmpty()) {
            this.initTask = new boolean[context.currentParallelism()];
        }
    }

    @Override
    public void start() {
        consumer = new InnerConsumerImpl(configuration);
        consumer.start();

        if (partitionDiscoveryIntervalMs > 0) {
            log.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    groupId,
                    partitionDiscoveryIntervalMs);

            context.callAsync(
                    this::requestServiceDiscovery,
                    this::handleSourceQueueChange,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            log.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    groupId);

            context.callAsync(this::requestServiceDiscovery, this::handleSourceQueueChange);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the rocketmq source pushes splits eagerly, rather than act upon split requests
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<RocketMQSourceSplit> splits, int subtaskId) {
        SourceSplitChangeResult sourceSplitChangeResult =
                new SourceSplitChangeResult(new HashSet<>(splits));
        this.calculateSplitAssignment(sourceSplitChangeResult);
        // If the failed subtask has already restarted, we need to assign splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            sendSplitChangesToRemote(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        log.debug(
                "Adding reader {} to RocketMQSourceEnumerator for consumer group {}.",
                subtaskId,
                groupId);
        sendSplitChangesToRemote(Collections.singleton(subtaskId));
        if (this.boundedness == Boundedness.BOUNDED) {
            // for RocketMQ bounded source,
            // send this signal to ensure the task can end after all the splits assigned are
            // completed.
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public RocketMQSourceEnumState snapshotState(long checkpointId) {
        return new RocketMQSourceEnumState(allocatedSet);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
            } catch (Exception e) {
                log.error("Shutdown rocketmq internal consumer error", e);
            }
        }
    }

    @Override
    public void handleSourceEvent(int taskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SourceCheckEvent) {
            handleCheckEvent(taskId, (SourceCheckEvent) sourceEvent);
        } else if (sourceEvent instanceof SourceReportOffsetEvent) {
            handleOffsetEvent(taskId, (SourceReportOffsetEvent) sourceEvent);
        } else if (sourceEvent instanceof SourceInitAssignEvent) {
            handleInitAssignEvent(taskId, (SourceInitAssignEvent) sourceEvent);
        }
    }

    // ----------------- private methods -------------------

    private Set<MessageQueue> requestServiceDiscovery() {
        // Ensure all subtasks have been initialized
        boolean initFinish = true;
        try {
            for (int i = 0; i < context.currentParallelism(); i++) {
                if (!initTask[i]) {
                    initFinish = false;
                    context.sendEventToSourceReader(i, new SourceDetectEvent());
                }
            }
        } catch (Exception e) {
            log.error("init request resend error, please check task has started", e);
            return Collections.emptySet();
        }
        if (!initFinish) {
            return Collections.emptySet();
        }

        Set<String> topicSet =
                Sets.newHashSet(
                        configuration
                                .getString(RocketMQSourceOptions.TOPIC)
                                .split(RocketMQSourceOptions.TOPIC_SEPARATOR));

        return topicSet.stream()
                .flatMap(
                        topic -> {
                            try {
                                return consumer.fetchMessageQueues(topic).get().stream();
                            } catch (Exception e) {
                                log.error(
                                        "Request topic route for service discovery error, topic={}",
                                        topic,
                                        e);
                            }
                            return Stream.empty();
                        })
                .collect(Collectors.toSet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSourceQueueChange(Set<MessageQueue> latestSet, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle source splits change due to ", t);
        }

        final SourceChangeResult sourceChangeResult = getSourceChangeResult(latestSet);
        if (sourceChangeResult.isEmpty()) {
            log.debug("Skip handle source allocated due to not queue change");
            return;
        }

        context.callAsync(
                () -> initializeSourceSplits(sourceChangeResult), this::handleSplitChanges);
    }

    // This method should only be invoked in the coordinator executor thread.
    private SourceSplitChangeResult initializeSourceSplits(SourceChangeResult sourceChangeResult) {
        lock.lock();

            Set<MessageQueue> increaseSet = sourceChangeResult.getIncreaseSet();
            Set<MessageQueue> decreaseSet = sourceChangeResult.getDecreaseSet();

            OffsetsSelector.MessageQueueOffsetsRetriever offsetsRetriever =
                    new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);

            Map<MessageQueue, Long> increaseStartingOffsets =
                    startingOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);
            Map<MessageQueue, Long> increaseStoppingOffsets =
                    stoppingOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);
            Map<MessageQueue, Long> decreaseStoppingOffsets =
                    stoppingOffsetsSelector.getMessageQueueOffsets(decreaseSet, offsetsRetriever);
            Map<MessageQueue, Long> decreaseStartingOffsets =
                    startingOffsetsSelector.getMessageQueueOffsets(decreaseSet, offsetsRetriever);

            Set<RocketMQSourceSplit> increaseSplitSet =
                    increaseSet.stream()
                            .map(
                                    mq -> {
                                        long startingOffset = increaseStartingOffsets.get(mq);
                                        long stoppingOffset =
                                                increaseStoppingOffsets.getOrDefault(
                                                        mq, RocketMQSourceSplit.NO_STOPPING_OFFSET);
                                        // Update cache
                                        checkedOffsets.put(mq, startingOffset);
                                        return new RocketMQSourceSplit(
                                                mq, startingOffset, stoppingOffset);
                                    })
                            .collect(Collectors.toSet());
            Set<RocketMQSourceSplit> decreaseSplitSet =
                    decreaseSet.stream()
                            .map(
                                    mq -> {
                                        long startingOffset = decreaseStartingOffsets.get(mq);
                                        long stoppingOffset =
                                                decreaseStoppingOffsets.getOrDefault(
                                                        mq, RocketMQSourceSplit.NO_STOPPING_OFFSET);
                                        // Update cache
                                        checkedOffsets.remove(mq);
                                        reflectedQueueToTaskId.remove(mq);
                                        allocatedSet.remove(mq);
                                        return new RocketMQSourceSplit(
                                                mq, startingOffset, stoppingOffset, false);
                                    })
                            .collect(Collectors.toSet());
            return new SourceSplitChangeResult(increaseSplitSet, decreaseSplitSet);

    }

    /**
     * Mark partition splits initialized by {@link
     * RocketMQSourceEnumerator#initializeSourceSplits(SourceChangeResult)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param sourceSplitChangeResult Partition split changes
     * @param t Exception in worker thread
     */
    private void handleSplitChanges(SourceSplitChangeResult sourceSplitChangeResult, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (partitionDiscoveryIntervalMs <= 0) {
            log.info("Split changes, but dynamic partition discovery is disabled.");
        }
        try {
            this.calculateSplitAssignment(sourceSplitChangeResult);
            this.sendSplitChangesToRemote(context.registeredReaders().keySet());
        } finally {
            lock.unlock();
        }
    }

    /** Calculate new split assignment according allocate strategy */
    private void calculateSplitAssignment(SourceSplitChangeResult sourceSplitChangeResult) {
        Map<Integer, Set<RocketMQSourceSplit>> newSourceSplitAllocateMap;

        // Preliminary calculation of distribution results
        {
            // Allocate valid queues
            if (sourceSplitChangeResult.decreaseSet != null && !sourceSplitChangeResult.decreaseSet.isEmpty()) {
                partitionId = 0;

                // Re-load balancing
                Set<RocketMQSourceSplit> allMQ = new HashSet<>();
                OffsetsSelector.MessageQueueOffsetsRetriever offsetsRetriever =
                        new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);
                Map<MessageQueue, Long> stoppingOffsets =
                        stoppingOffsetsSelector.getMessageQueueOffsets(allocatedSet, offsetsRetriever);

                // Calculate all queue
                allMQ.addAll(sourceSplitChangeResult.getIncreaseSet());
                allocatedSet.forEach(mq -> {
                    Set<MessageQueue> delete = sourceSplitChangeResult.decreaseSet.stream().map(RocketMQSourceSplit::getMessageQueue).collect(Collectors.toSet());
                    if (!delete.contains(mq)) {
                        allMQ.add(new RocketMQSourceSplit(mq, checkedOffsets.get(mq), stoppingOffsets.getOrDefault(mq, RocketMQSourceSplit.NO_STOPPING_OFFSET)));
                    }
                });
                newSourceSplitAllocateMap = this.allocateStrategy.allocate(allMQ, context.currentParallelism(), partitionId);

                // Update cache
                assignedMap = new ConcurrentHashMap<>(newSourceSplitAllocateMap);
                partitionId = allMQ.size();
            } else {
                newSourceSplitAllocateMap = this.allocateStrategy.allocate(
                        sourceSplitChangeResult.getIncreaseSet(), context.currentParallelism(), partitionId);

                // Update cache
                newSourceSplitAllocateMap.forEach((k, v) -> v.forEach(mq -> assignedMap.computeIfAbsent(k, r -> new HashSet<>()).add(mq)));
                partitionId += sourceSplitChangeResult.getIncreaseSet().size();
            }

            // Allocate deleted queues
            if (sourceSplitChangeResult.decreaseSet != null && !sourceSplitChangeResult.decreaseSet.isEmpty()) {
                sourceSplitChangeResult.decreaseSet.forEach(mq -> newSourceSplitAllocateMap.computeIfAbsent(reflectedQueueToTaskId.get(mq), k -> new HashSet<>()).add(mq));
            }
        }

        {
            // Calculate the result after queue migration
            if (sourceSplitChangeResult.decreaseSet != null && !sourceSplitChangeResult.decreaseSet.isEmpty()) {
                Map<Integer, Set<RocketMQSourceSplit>> migrationQueue = new HashMap<>();
                Map<Integer, Set<RocketMQSourceSplit>> noMigrationQueue = new HashMap<>();
                for (Map.Entry entry : newSourceSplitAllocateMap.entrySet()) {
                    int taskId = (int) entry.getKey();
                    Set<RocketMQSourceSplit> splits = (Set<RocketMQSourceSplit>) entry.getValue();
                    for (RocketMQSourceSplit split : splits) {
                        if (!split.getIsIncrease()) continue;
                        if (taskId != reflectedQueueToTaskId.get(split.getMessageQueue())) {
                            migrationQueue.computeIfAbsent(taskId, k -> new HashSet<>()).add(
                                    new RocketMQSourceSplit(split.getMessageQueue(), split.getStartingOffset(), split.getStoppingOffset(), false)
                            );
                        } else {
                            noMigrationQueue.computeIfAbsent(taskId, k -> new HashSet<>()).add(split);
                        }
                    }
                }

                // finally result
                migrationQueue.forEach((taskId, splits) -> {
                    newSourceSplitAllocateMap.computeIfAbsent(taskId, k -> new HashSet<>()).addAll(splits);
                });
                noMigrationQueue.forEach((taskId, splits) -> {
                    newSourceSplitAllocateMap.computeIfAbsent(taskId, k -> new HashSet<>()).removeAll(splits);
                });
            }
        }

        for (Map.Entry<Integer, Set<RocketMQSourceSplit>> entry :
                newSourceSplitAllocateMap.entrySet()) {
            this.pendingSplitAssignmentMap
                    .computeIfAbsent(entry.getKey(), r -> new HashSet<>())
                    .addAll(entry.getValue());
        }
    }

    // This method should only be invoked in the coordinator executor thread.
    private void sendSplitChangesToRemote(Set<Integer> pendingReaders) {
        Map<Integer, List<RocketMQSourceSplit>> incrementalSplit = new ConcurrentHashMap<>();

        for (Integer pendingReader : pendingReaders) {
            if (!context.registeredReaders().containsKey(pendingReader)) {
                throw new IllegalStateException(
                        String.format(
                                "Reader %d is not registered to source coordinator",
                                pendingReader));
            }

            final Set<RocketMQSourceSplit> pendingAssignmentForReader =
                    this.pendingSplitAssignmentMap.remove(pendingReader);

            // Put pending assignment into incremental assignment
            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                incrementalSplit
                        .computeIfAbsent(pendingReader, k -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);
                pendingAssignmentForReader.forEach(
                        split -> {
                            if (split.getIsIncrease()) {
                                this.allocatedSet.add(split.getMessageQueue());
                                reflectedQueueToTaskId.put(split.getMessageQueue(), pendingReader);
                            }
                        });
            }
        }

        // Assign pending splits to readers
        if (!incrementalSplit.isEmpty()) {
            log.info(
                    "Enumerator assigning split(s) to readers {}",
                    JSON.toJSONString(incrementalSplit, false));
            context.assignSplits(new SplitsAssignment<>(incrementalSplit));
        }

        // Sends NoMoreSplitsEvent to the readers if there is no more partition.
        if (partitionDiscoveryIntervalMs <= 0 && this.boundedness == Boundedness.BOUNDED) {
            log.info(
                    "No more rocketmq partition to assign. "
                            + "Sending NoMoreSplitsEvent to the readers in consumer group {}.",
                    groupId);
            pendingReaders.forEach(this.context::signalNoMoreSplits);
        }
    }

    private void handleInitAssignEvent(int taskId, SourceInitAssignEvent initAssignEvent) {
        if (this.initTask[taskId - 1]) {
            return;
        }

        // sync assign result
        if (initAssignEvent.getSplits() != null && !initAssignEvent.getSplits().isEmpty()) {
            log.info(
                    "Received SourceInitAssignEvent from reader {} with {} splits.",
                    taskId,
                    initAssignEvent.getSplits().toString());
            initAssignEvent
                    .getSplits()
                    .forEach(
                            split -> {
                                this.assignedMap
                                        .computeIfAbsent(taskId, r -> new HashSet<>())
                                        .add(split);
                                this.checkedOffsets.put(
                                        split.getMessageQueue(), split.getStoppingOffset());
                            });
        }
        this.initTask[taskId - 1] = true;
    }

    private void handleOffsetEvent(int taskId, SourceReportOffsetEvent sourceReportOffsetEvent) {
        lock.lock();
        try {
            // Update offset of message queue
            if (sourceReportOffsetEvent != null && sourceReportOffsetEvent.getCheckpoint() != -1) {
                log.info(
                        "Received SourceReportOffsetEvent from reader {} with offset {}",
                        taskId,
                        sourceReportOffsetEvent.getCheckpoint());
                MessageQueue mq =
                        new MessageQueue(
                                sourceReportOffsetEvent.getTopic(),
                                sourceReportOffsetEvent.getBroker(),
                                sourceReportOffsetEvent.getQueueId());
                this.checkedOffsets.put(mq, sourceReportOffsetEvent.getCheckpoint());
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleCheckEvent(int taskId, SourceCheckEvent sourceCheckEvent) {
        // Checks whether the actual assignment is consistent with the mapping
        lock.lock();
        try {
            if (sourceCheckEvent != null) {
                log.info(
                        "Received SourceCheckEvent from reader {} with offset {}",
                        taskId,
                        sourceCheckEvent.getAssignedMq());
                Map<Integer, List<RocketMQSourceSplit>> assigningMQ;
                Set<MessageQueue> idealAssignment = this.assignedMap.get(taskId).stream().map(r -> r.getMessageQueue()).collect(Collectors.toSet());
                Set<MessageQueue> actualAssignment = sourceCheckEvent.getAssignedMq();
                List<MessageQueue> increase =
                        new LinkedList<>(Sets.difference(idealAssignment, actualAssignment));
                List<MessageQueue> delete =
                        new LinkedList<>(Sets.difference(actualAssignment, idealAssignment));

                if (!increase.isEmpty() || !delete.isEmpty()) {
                    log.info(
                            "Received SourceCheckEvent from reader {} with offset {}, "
                                    + "ideal assignment {}, actual assignment {}, "
                                    + "increase {}, decrease {}",
                            taskId,
                            sourceCheckEvent.getAssignedMq(),
                            idealAssignment,
                            actualAssignment,
                            increase,
                            delete);

                    OffsetsSelector.MessageQueueOffsetsRetriever offsetsRetriever =
                            new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);
                    Map<MessageQueue, Long> decreaseStop =
                            stoppingOffsetsSelector.getMessageQueueOffsets(delete, offsetsRetriever);
                    Map<MessageQueue, Long> increaseStop =
                            stoppingOffsetsSelector.getMessageQueueOffsets(increase, offsetsRetriever);

                    // Support for decrease and increase
                    assigningMQ = new HashMap<>();
                    increase.forEach(
                            mq ->
                                    assigningMQ
                                            .computeIfAbsent(taskId, r -> new LinkedList<>())
                                            .add(
                                                    new RocketMQSourceSplit(
                                                            mq,
                                                            checkedOffsets.get(mq),
                                                            increaseStop.getOrDefault(
                                                                    mq,
                                                                    RocketMQSourceSplit
                                                                            .NO_STOPPING_OFFSET))));
                    delete.forEach(
                            mq ->
                                    assigningMQ
                                            .computeIfAbsent(taskId, r -> new LinkedList<>())
                                            .add(
                                                    new RocketMQSourceSplit(
                                                            mq,
                                                            checkedOffsets.get(mq),
                                                            decreaseStop.getOrDefault(
                                                                    mq,
                                                                    RocketMQSourceSplit
                                                                            .NO_STOPPING_OFFSET),
                                                            false)));

                    log.info(
                            "Enumerator check : reassigning split(s) to readers {}",
                            JSON.toJSONString(assigningMQ, false));
                    context.assignSplits(new SplitsAssignment<>(assigningMQ));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    private static class SourceChangeResult {
        private final Set<MessageQueue> increaseSet;
        private final Set<MessageQueue> decreaseSet;

        public SourceChangeResult(Set<MessageQueue> increaseSet, Set<MessageQueue> decreaseSet) {
            this.increaseSet = increaseSet;
            this.decreaseSet = decreaseSet;
        }

        public Set<MessageQueue> getIncreaseSet() {
            return increaseSet;
        }

        public Set<MessageQueue> getDecreaseSet() {
            return decreaseSet;
        }

        public boolean isEmpty() {
            return increaseSet.isEmpty() && decreaseSet.isEmpty();
        }
    }

    @VisibleForTesting
    public static class SourceSplitChangeResult {

        private final Set<RocketMQSourceSplit> increaseSet;
        private final Set<RocketMQSourceSplit> decreaseSet;

        private SourceSplitChangeResult(Set<RocketMQSourceSplit> increaseSet) {
            this.increaseSet = Collections.unmodifiableSet(increaseSet);
            this.decreaseSet = Sets.newHashSet();
        }

        private SourceSplitChangeResult(
                Set<RocketMQSourceSplit> increaseSet, Set<RocketMQSourceSplit> decreaseSet) {
            this.increaseSet = Collections.unmodifiableSet(increaseSet);
            this.decreaseSet = Collections.unmodifiableSet(decreaseSet);
        }

        public Set<RocketMQSourceSplit> getIncreaseSet() {
            return increaseSet;
        }

        public Set<RocketMQSourceSplit> getDecreaseSet() {
            return decreaseSet;
        }
    }

    @VisibleForTesting
    private SourceChangeResult getSourceChangeResult(Set<MessageQueue> latestSet) {
        Set<MessageQueue> currentSet = Collections.unmodifiableSet(this.allocatedSet);
        Set<MessageQueue> increaseSet = Sets.difference(latestSet, currentSet);
        Set<MessageQueue> decreaseSet = Sets.difference(currentSet, latestSet);

        SourceChangeResult changeResult = new SourceChangeResult(increaseSet, decreaseSet);

        // Current topic route is same as before
        if (changeResult.isEmpty()) {
            log.info(
                    "Request topic route for service discovery, current allocated queues size={}",
                    currentSet.size());
        } else {
            log.info(
                    "Request topic route for service discovery, current allocated queues size: {}. "
                            + "Changed details, current={}, latest={}, increase={}, decrease={}",
                    currentSet.size(),
                    currentSet,
                    latestSet,
                    increaseSet,
                    decreaseSet);
        }
        return changeResult;
    }
}
