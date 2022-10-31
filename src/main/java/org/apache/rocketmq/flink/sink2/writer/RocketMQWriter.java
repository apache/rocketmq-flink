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

package org.apache.rocketmq.flink.sink2.writer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.sink2.committer.RocketMQCommittable;
import org.apache.rocketmq.flink.sink2.committer.RocketMQCommitter;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQMessageSerializationSchema;
import org.apache.rocketmq.remoting.exception.RemotingException;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RocketMQWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, RocketMQWriterState>,
                PrecommittingSinkWriter<IN, RocketMQCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);

    private final Properties rabbitmqProducerConfig;

    private final SendCallback sendCallback;

    private final DeliveryGuarantee deliveryGuarantee;

    private final RocketMQWriterState rocketMQWriterState;

    private final RocketMQMessageSerializationSchema<IN> recordSerializer;

    private final RocketMQMessageSerializationSchema.RocketMQSinkContext rocketMQSinkContext;
    private DefaultMQProducer currentProducer;

    private final ArrayDeque<RocketMQCommittable> pendingCommittables;

    private final SinkWriterMetricGroup metricGroup;
    private final Counter numRecordsSendErrorsCounter;
    private final Counter numRecordsSendCounter;
    private final Counter numBytesSendCounter;
    private final String producerGroup;

    public RocketMQWriter(
            InitContext sinkInitContext,
            Properties rabbitmqProducerConfig,
            RocketMQMessageSerializationSchema<IN> recordSerializer,
            InitializationContext schemaContext,
            DeliveryGuarantee deliveryGuarantee,
            String producerGroup,
            Collection<RocketMQWriterState> recoveredStates) {
        checkNotNull(sinkInitContext, "sinkInitContext");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.producerGroup = checkNotNull(producerGroup, "producerGroup");
        this.rabbitmqProducerConfig =
                checkNotNull(rabbitmqProducerConfig, "rabbitmqProducerConfig");
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            this.currentProducer =
                    new TransactionMQProducer(
                            producerGroup, RocketMQConfig.buildAclRPCHook(rabbitmqProducerConfig));
            ((TransactionMQProducer) currentProducer)
                    .setTransactionListener(new RocketMQCommitter());
        } else if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE
                || deliveryGuarantee == DeliveryGuarantee.NONE) {
            this.currentProducer =
                    new DefaultMQProducer(
                            producerGroup, RocketMQConfig.buildAclRPCHook(rabbitmqProducerConfig));
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported RocketMQ writer semantic " + this.deliveryGuarantee);
        }

        RocketMQConfig.buildProducerConfigs(rabbitmqProducerConfig, currentProducer);

        try {
            this.currentProducer.start();
        } catch (MQClientException e) {
            LOG.error("Flink sink init failed, due to the producer cannot be initialized.");
            throw new RuntimeException(e);
        }

        this.rocketMQSinkContext =
                new DefaultRocketMQSinkContext(
                        sinkInitContext.getSubtaskId(),
                        sinkInitContext.getNumberOfParallelSubtasks(),
                        rabbitmqProducerConfig);

        try {
            recordSerializer.open(schemaContext, rocketMQSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.metricGroup = sinkInitContext.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
        this.rocketMQWriterState = new RocketMQWriterState(producerGroup);
        this.sendCallback = new WriterCallback(sinkInitContext.getMailboxExecutor());
        this.pendingCommittables = new ArrayDeque<>();
    }

    @Override
    public Collection<RocketMQCommittable> prepareCommit()
            throws IOException, InterruptedException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            final List<RocketMQCommittable> committables =
                    new ArrayList<>(pendingCommittables.size());
            while (!pendingCommittables.isEmpty()) {
                committables.add(pendingCommittables.poll());
            }
            LOG.debug("Committing {} committables.", committables);
            return committables;
        }
        return Collections.emptyList();
    }

    @Override
    public List<RocketMQWriterState> snapshotState(long checkpointId) throws IOException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            currentProducer =
                    new TransactionMQProducer(
                            producerGroup, RocketMQConfig.buildAclRPCHook(rabbitmqProducerConfig));
        }
        return ImmutableList.of(rocketMQWriterState);
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        final Message message =
                recordSerializer.serialize(element, rocketMQSinkContext, context.timestamp());
        try {
            currentProducer.send(message, sendCallback);
            numRecordsSendCounter.inc();
            System.out.println("send write");
        } catch (MQClientException | RemotingException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
        }
    }

    @Override
    public void close() throws Exception {}

    private class WriterCallback implements SendCallback {

        private final MailboxExecutor mailboxExecutor;

        public WriterCallback(MailboxExecutor mailboxExecutor) {
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            pendingCommittables.add(new RocketMQCommittable(producerGroup, sendResult.getMsgId()));
            System.out.println("send success" + sendResult.getMsgId());
            sendResult.getMsgId();
        }

        @Override
        public void onException(Throwable e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }
}
