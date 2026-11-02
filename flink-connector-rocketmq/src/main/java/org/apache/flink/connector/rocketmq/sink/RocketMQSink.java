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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.sink.committer.RocketMQCommitter;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittableSerializer;
import org.apache.flink.connector.rocketmq.sink.writer.RocketMQWriter;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.rocketmq.common.selector.MessageQueueSelector;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.OptionalLong;

@PublicEvolving
public class RocketMQSink<IN> implements Sink<IN>, SupportsCommitter<SendCommittable> {

    private final Configuration configuration;
    private final MessageQueueSelector messageQueueSelector;
    private final RocketMQSerializationSchema<IN> serializationSchema;

    RocketMQSink(
            Configuration configuration,
            MessageQueueSelector messageQueueSelector,
            RocketMQSerializationSchema<IN> serializationSchema) {
        this.configuration = configuration;
        this.messageQueueSelector = messageQueueSelector;
        this.serializationSchema = serializationSchema;
    }

    /**
     * Create a {@link RocketMQSinkBuilder} to construct a new {@link RocketMQSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link RocketMQSinkBuilder}
     */
    public static <IN> RocketMQSinkBuilder<IN> builder() {
        return new RocketMQSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) {
        return new RocketMQWriter<>(
                configuration, messageQueueSelector, serializationSchema, context);
    }

    /**
     * Flink 1.x still declares the deprecated {@link Sink.InitContext} overload abstract. The 1.x
     * runtime calls {@link #createWriter(WriterInitContext)}; this bridge serves callers that pass
     * the old context type. Both interfaces expose the same methods.
     */
    @Override
    @Deprecated
    public SinkWriter<IN> createWriter(Sink.InitContext context) {
        return createWriter(new InitContextAdapter(context));
    }

    private static final class InitContextAdapter implements WriterInitContext {

        private final Sink.InitContext delegate;

        private InitContextAdapter(Sink.InitContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return delegate.getUserCodeClassLoader();
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return delegate.getMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return delegate.getProcessingTimeService();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return delegate.metricGroup();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return delegate.asSerializationSchemaInitializationContext();
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return delegate.isObjectReuseEnabled();
        }

        @Override
        public <T> TypeSerializer<T> createInputSerializer() {
            return delegate.createInputSerializer();
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return delegate.getRestoredCheckpointId();
        }

        @Override
        public JobInfo getJobInfo() {
            return delegate.getJobInfo();
        }

        @Override
        public TaskInfo getTaskInfo() {
            return delegate.getTaskInfo();
        }
    }

    @Override
    public Committer<SendCommittable> createCommitter(CommitterInitContext context) {
        return new RocketMQCommitter(configuration);
    }

    @Override
    public SimpleVersionedSerializer<SendCommittable> getCommittableSerializer() {
        return new SendCommittableSerializer();
    }
}
