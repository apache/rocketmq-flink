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

package org.apache.flink.connector.rocketmq.grpc.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The split enumerator for the RocketMQ gRPC source.
 *
 * <p>The Pop model performs message-level load balancing on the broker side, so there is nothing to
 * distribute: every reader consumes all configured topics and the broker spreads messages across
 * the consumer group. The enumerator is therefore stateless and simply hands each registered reader
 * a single placeholder split, which is what triggers that reader to start its pop loop.
 */
@Internal
public class RocketMQGrpcSourceEnumerator
        implements SplitEnumerator<RocketMQGrpcSourceSplit, RocketMQGrpcSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQGrpcSourceEnumerator.class);

    private final SplitEnumeratorContext<RocketMQGrpcSourceSplit> context;

    public RocketMQGrpcSourceEnumerator(SplitEnumeratorContext<RocketMQGrpcSourceSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
        // No topic discovery or split distribution is required for the Pop model.
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Splits are assigned proactively when a reader registers.
    }

    @Override
    public void addReader(int subtaskId) {
        context.assignSplit(new RocketMQGrpcSourceSplit(), subtaskId);
        context.signalNoMoreSplits(subtaskId);
        LOG.info("Assigned the placeholder split to reader {}", subtaskId);
    }

    @Override
    public void addSplitsBack(List<RocketMQGrpcSourceSplit> splits, int subtaskId) {
        // The placeholder split is stateless and is re-assigned when the reader re-registers, so
        // returned splits can be dropped.
        LOG.info("RocketMQ gRPC source dropped returned placeholder splits: {}", splits);
    }

    @Override
    public RocketMQGrpcSourceEnumState snapshotState(long checkpointId) {
        return new RocketMQGrpcSourceEnumState();
    }

    @Override
    public void close() {
        // Nothing to close.
    }
}
