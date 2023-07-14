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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.KEY_PREFIX;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_1;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_2;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.TAGS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.getAclRpcHook;

public class SimpleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {

        DefaultMQProducer producer =
                new DefaultMQProducer(ConnectorConfig.PRODUCER_GROUP, getAclRpcHook(), true, null);
        producer.setNamesrvAddr(ENDPOINTS);
        producer.setAccessChannel(AccessChannel.CLOUD);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        send(producer, SOURCE_TOPIC_1);
        send(producer, SOURCE_TOPIC_2);

        producer.shutdown();
    }

    private static void send(MQProducer producer, String topic) {
        for (int i = 0; i < ConnectorConfig.MESSAGE_NUM; i++) {
            String content = "Test Message " + i;
            Message msg = new Message(topic, TAGS, KEY_PREFIX + i, content.getBytes());
            try {
                SendResult sendResult = producer.send(msg);
                assert sendResult != null;
                System.out.printf(
                        "Send result: topic=%s, msgId=%s, brokerName=%s, queueId=%s\n",
                        topic,
                        sendResult.getMsgId(),
                        sendResult.getMessageQueue().getBrokerName(),
                        sendResult.getMessageQueue().getQueueId());
            } catch (Exception e) {
                LOGGER.info("Send message failed. {}", e.toString());
            }
        }
    }
}
