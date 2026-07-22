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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ACCESS_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.CONSUMER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.PRODUCER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SECRET_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SINK_TOPIC_2;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_2;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.getAclRpcHook;
import static org.junit.Assert.assertTrue;

/**
 * SQL DDL end-to-end integration test against a real RocketMQ instance.
 *
 * <p>Pipeline: raw producer → {@code flink-source-2} → SQL source table → INSERT INTO SQL sink
 * table → {@code flink-sink-2} → raw consumer verification.
 *
 * <p>Skipped automatically when environment variables are not set.
 */
public class SqlIntegrationTest {

    private static final int MESSAGE_COUNT = 10;
    private static final String MESSAGE_PREFIX = "sql-test-" + System.currentTimeMillis() + "-";

    @Before
    public void checkEnv() {
        Assume.assumeTrue("Skipping: ROCKETMQ_NAMESRV_ADDR not set", ENDPOINTS != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_ACCESS_KEY not set", ACCESS_KEY != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_SECRET_KEY not set", SECRET_KEY != null);
    }

    @Test
    public void sqlSourceToSinkTest() throws Exception {
        System.out.println("=== RocketMQ SQL DDL Integration Test ===");
        System.out.println("Source topic: " + SOURCE_TOPIC_2);
        System.out.println("Sink topic  : " + SINK_TOPIC_2);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE rmq_source (msg STRING) WITH ("
                        + "'connector' = 'rocketmq',"
                        + ("'rocketmq.client.endpoints' = '" + ENDPOINTS + "',")
                        + ("'rocketmq.source.topic' = '" + SOURCE_TOPIC_2 + "',")
                        + ("'rocketmq.source.group' = '" + CONSUMER_GROUP + "',")
                        + ("'rocketmq.client.accessKey' = '" + ACCESS_KEY + "',")
                        + ("'rocketmq.client.secretKey' = '" + SECRET_KEY + "',")
                        + "'rocketmq.source.startup.scan.mode' = 'latest'"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE rmq_sink (msg STRING) WITH ("
                        + "'connector' = 'rocketmq',"
                        + ("'rocketmq.client.endpoints' = '" + ENDPOINTS + "',")
                        + ("'rocketmq.sink.topic' = '" + SINK_TOPIC_2 + "',")
                        + ("'rocketmq.sink.group' = '" + PRODUCER_GROUP + "',")
                        + ("'rocketmq.client.accessKey' = '" + ACCESS_KEY + "',")
                        + ("'rocketmq.client.secretKey' = '" + SECRET_KEY + "'")
                        + ")");

        System.out.println("[Step 1] Submitting INSERT INTO job...");
        TableResult result = tEnv.executeSql("INSERT INTO rmq_sink SELECT msg FROM rmq_source");

        System.out.println("[Wait] Giving SQL source 15s to initialize...");
        Thread.sleep(15_000);

        produceMessages();

        System.out.println("[Wait] Giving pipeline 20s to process...");
        Thread.sleep(20_000);

        result.getJobClient()
                .ifPresent(
                        client -> {
                            try {
                                client.cancel().get(10, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (Exception e) {
                                System.out.println(
                                        "[Flink] Job not cancellable, checking failure cause...");
                                try {
                                    client.getJobExecutionResult()
                                            .get(5, java.util.concurrent.TimeUnit.SECONDS);
                                } catch (Exception cause) {
                                    cause.printStackTrace(System.out);
                                }
                            }
                        });

        verifySinkMessages();
        System.out.println("\n=== SQL DDL Integration Test PASSED ===");
    }

    private void produceMessages() throws Exception {
        System.out.println("[Step 2] Producing " + MESSAGE_COUNT + " messages to " + SOURCE_TOPIC_2);
        DefaultMQProducer producer =
                new DefaultMQProducer(PRODUCER_GROUP, getAclRpcHook(), true, null);
        producer.setNamesrvAddr(ENDPOINTS);
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.start();
        try {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                producer.send(
                        new Message(SOURCE_TOPIC_2, "*", (MESSAGE_PREFIX + i).getBytes()), 5000);
            }
        } finally {
            producer.shutdown();
        }
        System.out.println("[Step 2] Done");
    }

    private void verifySinkMessages() throws Exception {
        System.out.println("[Step 3] Verifying messages in " + SINK_TOPIC_2);
        DefaultLitePullConsumer consumer =
                new DefaultLitePullConsumer(CONSUMER_GROUP, getAclRpcHook());
        consumer.setNamesrvAddr(ENDPOINTS);
        consumer.setAccessChannel(AccessChannel.CLOUD);
        consumer.setAutoCommit(false);
        consumer.setVipChannelEnabled(false);
        consumer.start();

        List<String> received = new ArrayList<>();
        try {
            Collection<MessageQueue> queues = consumer.fetchMessageQueues(SINK_TOPIC_2);
            consumer.assign(queues);
            for (MessageQueue mq : queues) {
                consumer.seek(mq, Math.max(0, consumer.offsetForTimestamp(mq, 0L)));
            }
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline && received.size() < MESSAGE_COUNT) {
                List<MessageExt> batch = consumer.poll(3000);
                for (MessageExt msg : batch) {
                    String body = new String(msg.getBody());
                    if (body.contains(MESSAGE_PREFIX)) {
                        received.add(body);
                    }
                }
            }
        } finally {
            consumer.shutdown();
        }
        System.out.printf(
                "[Step 3] Verified: %d/%d messages received in sink topic%n",
                received.size(), MESSAGE_COUNT);
        assertTrue(
                "Expected " + MESSAGE_COUNT + " messages in sink, got " + received.size(),
                received.size() >= MESSAGE_COUNT);
    }
}
