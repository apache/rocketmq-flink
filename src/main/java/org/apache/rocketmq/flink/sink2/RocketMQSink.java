/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.sink2;

import org.apache.rocketmq.flink.sink2.committer.RocketMQCommittable;
import org.apache.rocketmq.flink.sink2.committer.RocketMQCommittableSerializer;
import org.apache.rocketmq.flink.sink2.committer.RocketMQCommitter;
import org.apache.rocketmq.flink.sink2.writer.RocketMQWriter;
import org.apache.rocketmq.flink.sink2.writer.RocketMQWriterState;
import org.apache.rocketmq.flink.sink2.writer.RocketMQWriterStateSerializer;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQMessageSerializationSchema;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class RocketMQSink<IN>
        implements StatefulSink<IN, RocketMQWriterState>,
                TwoPhaseCommittingSink<IN, RocketMQCommittable> {
    private final DeliveryGuarantee deliveryGuarantee;

    private final Properties rabbitmqProducerConfig;

    private final String producerGroup;

    private final RocketMQMessageSerializationSchema<IN> recordSerializer;

    RocketMQSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties rabbitmqProducerConfig,
            String producerGroup,
            RocketMQMessageSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.rabbitmqProducerConfig = rabbitmqProducerConfig;
        this.producerGroup = producerGroup;
        this.recordSerializer = recordSerializer;
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
    public RocketMQWriter<IN> createWriter(InitContext context) throws IOException {

        return new RocketMQWriter<>(
                context,
                rabbitmqProducerConfig,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                deliveryGuarantee,
                producerGroup,
                Collections.emptyList());
    }

    @Override
    public RocketMQWriter<IN> restoreWriter(
            InitContext context, Collection<RocketMQWriterState> recoveredState)
            throws IOException {
        return new RocketMQWriter<>(
                context,
                rabbitmqProducerConfig,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                deliveryGuarantee,
                producerGroup,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<RocketMQWriterState> getWriterStateSerializer() {
        return new RocketMQWriterStateSerializer();
    }

    @Override
    public Committer<RocketMQCommittable> createCommitter() throws IOException {
        return new RocketMQCommitter();
    }

    @Override
    public SimpleVersionedSerializer<RocketMQCommittable> getCommittableSerializer() {
        return new RocketMQCommittableSerializer();
    }
}
