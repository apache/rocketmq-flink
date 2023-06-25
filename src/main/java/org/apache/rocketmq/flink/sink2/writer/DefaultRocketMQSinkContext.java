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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQMessageSerializationSchema;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Context providing information to assist constructing a {@link
 * org.apache.rocketmq.common.message.Message}.
 */
public class DefaultRocketMQSinkContext
        implements RocketMQMessageSerializationSchema.RocketMQSinkContext {

    private final int subtaskId;
    private final int numberOfParallelInstances;
    private final Properties rocketMQProducerConfig;
    private final Map<String, int[]> cachedPartitions = new HashMap<>();

    public DefaultRocketMQSinkContext(
            int subtaskId, int numberOfParallelInstances, Properties rocketMQProducerConfig) {
        this.subtaskId = subtaskId;
        this.numberOfParallelInstances = numberOfParallelInstances;
        this.rocketMQProducerConfig = rocketMQProducerConfig;
    }

    @Override
    public int getParallelInstanceId() {
        return subtaskId;
    }

    @Override
    public int getNumberOfParallelInstances() {
        return numberOfParallelInstances;
    }

    @Override
    public int[] getPartitionsForTopic(String topic) {
        return cachedPartitions.computeIfAbsent(topic, this::fetchPartitionsForTopic);
    }

    private int[] fetchPartitionsForTopic(String topic) {
        DefaultMQProducer producer = null;
        try {
            producer =
                    new DefaultMQProducer(RocketMQConfig.buildAclRPCHook(rocketMQProducerConfig));
            producer.start();
            List<MessageQueue> messageQueues = producer.fetchPublishMessageQueues(topic);
            return messageQueues.stream()
                    .sorted(Comparator.comparing(MessageQueue::getQueueId))
                    .map(MessageQueue::getQueueId)
                    .mapToInt(Integer::intValue)
                    .toArray();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } finally {
            if (producer != null) {
                producer.shutdown();
            }
        }
    }
}
