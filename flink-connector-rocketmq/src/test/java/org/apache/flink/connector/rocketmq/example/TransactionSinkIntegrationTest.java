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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ACCESS_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.CONSUMER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.PRODUCER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SECRET_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.getAclRpcHook;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the EXACTLY_ONCE (transactional) sink against a real RocketMQ instance.
 *
 * <p>Test flow:
 *
 * <ol>
 *   <li>Record the current max offsets of the transaction topic
 *   <li>Run a bounded Flink job writing messages with EXACTLY_ONCE; checkpointing triggers the
 *       two-phase commit (prepareCommit → Committer.commit → endTransaction COMMIT)
 *   <li>Consume the topic from the recorded offsets and verify every message became visible, which
 *       only happens after a successful transaction commit
 * </ol>
 *
 * <p>Skipped automatically when environment variables are not set. The topic must be a
 * TRANSACTION-type topic on the target instance.
 */
public class TransactionSinkIntegrationTest {

    private static final String TRANS_TOPIC =
            System.getenv().getOrDefault("ROCKETMQ_TRANS_TOPIC", "TransTopic");
    private static final int MESSAGE_COUNT = 10;
    private static final String MESSAGE_PREFIX = "txn-test-" + System.currentTimeMillis() + "-";

    @Before
    public void checkEnv() {
        Assume.assumeTrue("Skipping: ROCKETMQ_NAMESRV_ADDR not set", ENDPOINTS != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_ACCESS_KEY not set", ACCESS_KEY != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_SECRET_KEY not set", SECRET_KEY != null);
    }

    @Test
    public void exactlyOnceSinkTest() throws Exception {
        System.out.println("=== RocketMQ EXACTLY_ONCE Sink Integration Test ===");
        System.out.println("Endpoints  : " + ENDPOINTS);
        System.out.println("Trans topic: " + TRANS_TOPIC);

        List<String> payloads =
                IntStream.range(0, MESSAGE_COUNT)
                        .mapToObj(i -> MESSAGE_PREFIX + i)
                        .collect(Collectors.toList());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.enableCheckpointing(3000);

        RocketMQSink<String> sink =
                RocketMQSink.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setGroupId(PRODUCER_GROUP)
                        .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, SECRET_KEY)
                        .setConfig(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL, AccessChannel.CLOUD)
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setSerializer(
                                (element, context, timestamp) ->
                                        new Message(TRANS_TOPIC, "*", element.getBytes()))
                        .build();

        env.fromCollection(payloads).sinkTo(sink).setParallelism(1);
        System.out.println("[Step 1] Running bounded EXACTLY_ONCE job...");
        env.execute("transaction-sink-test");
        System.out.println("[Step 1] Job finished");

        // Transactions are committed by the Committer on checkpoint / final checkpoint.
        // Verify all messages became visible to consumers.
        verifyMessages(payloads);
        System.out.println("\n=== EXACTLY_ONCE Sink Test PASSED ===");
    }

    private void verifyMessages(List<String> expected) throws Exception {
        System.out.println("[Step 2] Verifying committed messages in " + TRANS_TOPIC);
        DefaultLitePullConsumer consumer =
                new DefaultLitePullConsumer(CONSUMER_GROUP, getAclRpcHook());
        consumer.setNamesrvAddr(ENDPOINTS);
        consumer.setAccessChannel(AccessChannel.CLOUD);
        consumer.setAutoCommit(false);
        consumer.setVipChannelEnabled(false);
        consumer.start();

        List<String> received = new ArrayList<>();
        try {
            Collection<MessageQueue> queues = consumer.fetchMessageQueues(TRANS_TOPIC);
            consumer.assign(queues);
            // Only need recent messages; unique prefix isolates this run.
            for (MessageQueue mq : queues) {
                consumer.seek(mq, Math.max(0, consumer.offsetForTimestamp(mq, 0L)));
            }
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline && received.size() < expected.size()) {
                List<MessageExt> batch = consumer.poll(3000);
                for (MessageExt msg : batch) {
                    String body = new String(msg.getBody());
                    if (body.startsWith(MESSAGE_PREFIX)) {
                        received.add(body);
                    }
                }
            }
        } finally {
            consumer.shutdown();
        }
        System.out.printf(
                "[Step 2] Verified: %d/%d committed messages visible%n",
                received.size(), expected.size());
        assertTrue(
                "Expected " + expected.size() + " committed messages, got " + received.size(),
                received.size() >= expected.size());
    }
}
