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

package org.apache.flink.connector.rocketmq.sink.writer;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.legacy.common.selector.MessageQueueSelector;
import org.apache.flink.connector.rocketmq.sink.InnerProducer;
import org.apache.flink.connector.rocketmq.sink.InnerProducerImpl;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContext;
import org.apache.flink.connector.rocketmq.sink.writer.context.RocketMQSinkContextImpl;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocketMQWriter<IN>
        implements TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, SendCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQWriter.class);
    private static final String RMQ_PRODUCER_METRIC_NAME = "RocketMQProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String ROCKETMQ_PRODUCER_METRICS = "producer-metrics";

    private final transient InnerProducer producer;

    private final DeliveryGuarantee deliveryGuarantee;
    private final MessageQueueSelector messageQueueSelector;
    private final RocketMQSinkContext rocketmqSinkContext;
    private final RocketMQSerializationSchema<IN> serializationSchema;
    private final Map<String, SendResult> sendResultMap;

    public RocketMQWriter(
            Configuration configuration,
            MessageQueueSelector messageQueueSelector,
            RocketMQSerializationSchema<IN> serializationSchema,
            Sink.InitContext initContext) {

        this.deliveryGuarantee =
                DeliveryGuarantee.valueOf(
                        configuration.getString(RocketMQSinkOptions.DELIVERY_GUARANTEE));
        this.messageQueueSelector = messageQueueSelector;
        this.serializationSchema = serializationSchema;
        this.rocketmqSinkContext = new RocketMQSinkContextImpl(initContext, configuration);
        this.sendResultMap = new ConcurrentHashMap<>();
        this.producer = new InnerProducerImpl(configuration);
        this.producer.start();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        try {
            Message message =
                    serializationSchema.serialize(
                            element, rocketmqSinkContext, System.currentTimeMillis());
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                producer.sendMessageInTransaction(message)
                        .whenComplete(
                                (sendResult, throwable) -> {
                                    sendResultMap.put(sendResult.getTransactionId(), sendResult);
                                });
            } else {
                producer.send(message)
                        .whenComplete(
                                (sendResult, throwable) -> {
                                    sendResultMap.put(sendResult.getTransactionId(), sendResult);
                                });
            }
        } catch (Exception e) {
            LOG.error("Send message error", e);
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // rocketmq client send message to server immediately, no need flush here
    }

    @Override
    public Collection<SendCommittable> prepareCommit() throws IOException, InterruptedException {
        LOG.info("Prepare commit");
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            final List<SendCommittable> committables = new ArrayList<>();
            for (SendResult sendResult : sendResultMap.values()) {
                committables.add(new SendCommittable(sendResult));
            }
            LOG.info("Committable size={}.", committables.size());
            sendResultMap.clear();
            return committables;
        }
        return Collections.emptyList();
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        TwoPhaseCommittingSink.PrecommittingSinkWriter.super.writeWatermark(watermark);
    }

    @Override
    public void close() throws Exception {}
}
