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

package org.apache.flink.connector.rocketmq.example;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_1;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_2;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.TAGS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.getAclRpcHook;

public class SimpleConsumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer(
                        ConnectorConfig.CONSUMER_GROUP,
                        getAclRpcHook(),
                        new AllocateMessageQueueAveragely());

        consumer.setNamesrvAddr(ENDPOINTS);
        consumer.setAccessChannel(AccessChannel.CLOUD);
        consumer.subscribe(SOURCE_TOPIC_1, TAGS);
        consumer.subscribe(SOURCE_TOPIC_2, TAGS);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(
                (MessageListenerConcurrently)
                        (msgList, context) -> {
                            for (MessageExt msg : msgList) {
                                System.out.printf(
                                        "Receive: topic=%s, msgId=%s, brokerName=%s, queueId=%s, payload=%s\n",
                                        msg.getTopic(),
                                        msg.getMsgId(),
                                        msg.getBrokerName(),
                                        msg.getQueueId(),
                                        new String(msg.getBody()));
                            }
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        });
        consumer.start();
    }
}
