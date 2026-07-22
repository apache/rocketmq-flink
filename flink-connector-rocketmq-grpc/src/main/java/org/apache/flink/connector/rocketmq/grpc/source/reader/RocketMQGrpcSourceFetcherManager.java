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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;

import java.util.function.Supplier;

/**
 * The {@link SingleThreadFetcherManager} for the RocketMQ gRPC source. The Pop source does not
 * acknowledge messages from the source side (acknowledgement is deferred to a downstream operator),
 * so this manager only provides a single fetcher thread that receives messages.
 */
@Internal
public class RocketMQGrpcSourceFetcherManager
        extends SingleThreadFetcherManager<MessageViewImpl, RocketMQGrpcSourceSplit> {

    public RocketMQGrpcSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageViewImpl>> elementsQueue,
            Supplier<SplitReader<MessageViewImpl, RocketMQGrpcSourceSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier, new Configuration());
    }
}
