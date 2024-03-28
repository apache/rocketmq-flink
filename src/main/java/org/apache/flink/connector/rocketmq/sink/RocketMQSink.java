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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.legacy.common.selector.MessageQueueSelector;
import org.apache.flink.connector.rocketmq.sink.committer.RocketMQCommitter;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittableSerializer;
import org.apache.flink.connector.rocketmq.sink.writer.RocketMQWriter;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;

@PublicEvolving
public class RocketMQSink<IN> implements TwoPhaseCommittingSink<IN, SendCommittable> {

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
    public PrecommittingSinkWriter<IN, SendCommittable> createWriter(InitContext context) {
        return new RocketMQWriter<>(
                configuration, messageQueueSelector, serializationSchema, context);
    }

    @Override
    public Committer<SendCommittable> createCommitter() {
        return new RocketMQCommitter(configuration);
    }

    @Override
    public SimpleVersionedSerializer<SendCommittable> getCommittableSerializer() {
        return new SendCommittableSerializer();
    }
}
