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
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplitState;

import java.util.Map;

/**
 * The source reader for the RocketMQ gRPC connector. It emits {@link AckableMessage} records and
 * does not acknowledge messages itself; acknowledgement is performed by a downstream operator using
 * the self-contained receipt handle carried by each emitted record. Un-acked messages are
 * redelivered by the broker after their invisible duration expires, providing the at-least-once
 * guarantee.
 */
@Internal
public class RocketMQGrpcSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                MessageViewImpl,
                AckableMessage<T>,
                RocketMQGrpcSourceSplit,
                RocketMQGrpcSourceSplitState> {

    public RocketMQGrpcSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageViewImpl>> elementsQueue,
            RocketMQGrpcSourceFetcherManager fetcherManager,
            RocketMQGrpcSourceRecordEmitter<T> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(elementsQueue, fetcherManager, recordEmitter, config, context);
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
