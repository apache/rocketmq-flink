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

package org.apache.flink.connector.rocketmq.grpc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessage;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessageTypeInfo;
import org.apache.flink.connector.rocketmq.grpc.source.deserialization.RocketMQGrpcDeserializationSchema;
import org.apache.flink.connector.rocketmq.grpc.source.enumerator.RocketMQGrpcSourceEnumState;
import org.apache.flink.connector.rocketmq.grpc.source.enumerator.RocketMQGrpcSourceEnumStateSerializer;
import org.apache.flink.connector.rocketmq.grpc.source.enumerator.RocketMQGrpcSourceEnumerator;
import org.apache.flink.connector.rocketmq.grpc.source.reader.CheckpointAckTracker;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageViewImpl;
import org.apache.flink.connector.rocketmq.grpc.source.reader.RocketMQGrpcSourceFetcherManager;
import org.apache.flink.connector.rocketmq.grpc.source.reader.RocketMQGrpcSourceReader;
import org.apache.flink.connector.rocketmq.grpc.source.reader.RocketMQGrpcSourceRecordEmitter;
import org.apache.flink.connector.rocketmq.grpc.source.reader.RocketMQGrpcSourceSplitReader;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplit;
import org.apache.flink.connector.rocketmq.grpc.source.split.RocketMQGrpcSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/** The gRPC ({@code rocketmq-client-java}) implementation of a FLIP-27 RocketMQ {@link Source}. */
@PublicEvolving
public class RocketMQGrpcSource<OUT>
        implements Source<
                        AckableMessage<OUT>, RocketMQGrpcSourceSplit, RocketMQGrpcSourceEnumState>,
                ResultTypeQueryable<AckableMessage<OUT>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQGrpcSource.class);

    private final Configuration configuration;
    private final Boundedness boundedness;
    private final RocketMQGrpcDeserializationSchema<OUT> deserializationSchema;

    RocketMQGrpcSource(
            Configuration configuration,
            Boundedness boundedness,
            RocketMQGrpcDeserializationSchema<OUT> deserializationSchema) {
        this.configuration = configuration;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
    }

    /** Get a {@link RocketMQGrpcSourceBuilder} to build a {@link RocketMQGrpcSource}. */
    public static <OUT> RocketMQGrpcSourceBuilder<OUT> builder() {
        return new RocketMQGrpcSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<AckableMessage<OUT>, RocketMQGrpcSourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        final FutureCompletingBlockingQueue<RecordsWithSplitIds<MessageViewImpl>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });

        final RocketMQGrpcSourceSplitReader splitReader =
                new RocketMQGrpcSourceSplitReader(configuration);
        final Supplier<SplitReader<MessageViewImpl, RocketMQGrpcSourceSplit>> splitReaderSupplier =
                () -> splitReader;

        final RocketMQGrpcSourceFetcherManager fetcherManager =
                new RocketMQGrpcSourceFetcherManager(elementsQueue, splitReaderSupplier);

        final ConsumerMode mode = configuration.get(RocketMQGrpcSourceOptions.MODE);
        final CheckpointAckTracker ackTracker =
                mode == ConsumerMode.SIMPLE ? new CheckpointAckTracker() : null;

        final RocketMQGrpcSourceRecordEmitter<OUT> recordEmitter =
                new RocketMQGrpcSourceRecordEmitter<>(
                        deserializationSchema,
                        configuration.get(RocketMQGrpcOptions.NAMESPACE),
                        configuration.get(RocketMQGrpcSourceOptions.CONSUMER_GROUP),
                        ackTracker == null ? null : ackTracker::add);

        // The same split reader instance is handed to the fetcher manager and to the source reader:
        // in SIMPLE mode the reader acks through the very consumer that received the messages.
        return new RocketMQGrpcSourceReader<>(
                elementsQueue,
                fetcherManager,
                recordEmitter,
                configuration,
                readerContext,
                ackTracker,
                splitReader);
    }

    @Override
    public SplitEnumerator<RocketMQGrpcSourceSplit, RocketMQGrpcSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RocketMQGrpcSourceSplit> enumContext) {
        return new RocketMQGrpcSourceEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<RocketMQGrpcSourceSplit, RocketMQGrpcSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RocketMQGrpcSourceSplit> enumContext,
            RocketMQGrpcSourceEnumState checkpoint) {
        LOG.info(
                "Restoring RocketMQ gRPC source enumerator from checkpoint; the Pop-based "
                        + "enumerator is stateless, so no split assignment state is recovered.");
        return new RocketMQGrpcSourceEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<RocketMQGrpcSourceSplit> getSplitSerializer() {
        return new RocketMQGrpcSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<RocketMQGrpcSourceEnumState>
            getEnumeratorCheckpointSerializer() {
        return new RocketMQGrpcSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<AckableMessage<OUT>> getProducedType() {
        return new AckableMessageTypeInfo<>(deserializationSchema.getProducedType());
    }
}
