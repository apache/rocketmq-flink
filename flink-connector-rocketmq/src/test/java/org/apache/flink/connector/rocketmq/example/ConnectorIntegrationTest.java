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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ACCESS_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.CONSUMER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.PRODUCER_GROUP;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SECRET_KEY;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SINK_TOPIC_1;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.SOURCE_TOPIC_1;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.TAGS;
import static org.apache.flink.connector.rocketmq.example.ConnectorConfig.getAclRpcHook;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the FLIP-27 Source and Sink APIs against a real RocketMQ instance.
 *
 * <p>Test flow:
 *
 * <ol>
 *   <li>Start the Flink pipeline (Source → Sink) in a daemon thread
 *   <li>Produce messages to the source topic — Source receives and forwards to Sink
 *   <li>Stop the Flink job after messages flow through
 *   <li>Verify messages arrived in the sink topic
 * </ol>
 *
 * <p>Skipped automatically when environment variables are not set.
 */
public class ConnectorIntegrationTest {

    private static final int MESSAGE_COUNT = 20;
    private static final String MESSAGE_PREFIX = "integration-test-";

    @Before
    public void checkEnv() {
        Assume.assumeTrue("Skipping: ROCKETMQ_NAMESRV_ADDR not set", ENDPOINTS != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_ACCESS_KEY not set", ACCESS_KEY != null);
        Assume.assumeTrue("Skipping: ROCKETMQ_SECRET_KEY not set", SECRET_KEY != null);
    }

    @Test
    public void sourceToSinkPipelineTest() throws Exception {
        System.out.println("=== RocketMQ Flink Connector Integration Test ===");
        System.out.println("Endpoints  : " + ENDPOINTS);
        System.out.println("Source topic: " + SOURCE_TOPIC_1);
        System.out.println("Sink topic  : " + SINK_TOPIC_1);
        System.out.println("Messages    : " + MESSAGE_COUNT);
        System.out.println();

        // Step 1: Start Flink pipeline in a daemon thread
        Thread jobThread = startFlinkPipeline();

        // Give the Source time to initialize and start waiting for messages
        System.out.println("[Wait] Giving Source 15s to initialize...");
        Thread.sleep(15_000);

        // Step 2: Produce messages — Source receives them and forwards to Sink
        produceMessages();

        // Give messages time to flow through the pipeline
        System.out.println("[Wait] Giving pipeline 15s to process messages...");
        Thread.sleep(15_000);

        // Step 3: Stop the Flink job
        System.out.println("[Step 3] Stopping Flink job...");
        jobThread.interrupt();
        jobThread.join(10_000);
        System.out.println("[Step 3] Job stopped");

        // Step 4: Verify messages arrived in sink topic
        verifySinkMessages();

        System.out.println("\n=== Integration Test PASSED ===");
    }

    // ---- Step 1: Start Flink Source → Sink pipeline ----

    private Thread startFlinkPipeline() throws Exception {
        System.out.println("[Step 1] Starting Flink pipeline: "
                + SOURCE_TOPIC_1 + " → " + SINK_TOPIC_1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // Build FLIP-27 Source (unbounded — receives messages as they arrive).
        // Start from the latest offsets: the pipeline starts before messages are produced,
        // and seeking to a fixed offset fails when the topic already holds data.
        RocketMQSource<String> source =
                RocketMQSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setGroupId(CONSUMER_GROUP)
                        .setTopics(SOURCE_TOPIC_1)
                        .setMinOffsets(OffsetsSelector.latest())
                        .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, SECRET_KEY)
                        .setConfig(
                                RocketMQOptions.OPTIONAL_ACCESS_CHANNEL, AccessChannel.CLOUD)
                        .setDeserializer(new StringBodyDeserializationSchema())
                        .build();

        // Build FLIP-27 Sink
        RocketMQSink<String> sink =
                RocketMQSink.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setGroupId(PRODUCER_GROUP)
                        .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, SECRET_KEY)
                        .setConfig(
                                RocketMQOptions.OPTIONAL_ACCESS_CHANNEL, AccessChannel.CLOUD)
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setSerializer(
                                (element, context, timestamp) ->
                                        new Message(SINK_TOPIC_1, TAGS, element.getBytes()))
                        .build();

        // Source → Sink pipeline
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rocketmq-source")
                .setParallelism(1)
                .sinkTo(sink)
                .setParallelism(1);

        // env.execute() blocks, so run in a daemon thread
        Thread jobThread =
                new Thread(
                        () -> {
                            try {
                                env.execute("integration-test-pipeline");
                            } catch (Exception e) {
                                System.out.println("[Flink] Job ended: " + e.getMessage());
                                e.printStackTrace(System.out);
                            }
                        },
                        "flink-job-thread");
        jobThread.setDaemon(true);
        jobThread.start();

        // Wait for the MiniCluster to start
        Thread.sleep(5000);
        System.out.println("[Step 1] Flink job started\n");

        return jobThread;
    }

    // ---- Step 2: Produce messages via raw RocketMQ client ----

    private void produceMessages() throws Exception {
        System.out.println(
                "[Step 2] Producing " + MESSAGE_COUNT + " messages to " + SOURCE_TOPIC_1);

        DefaultMQProducer producer =
                new DefaultMQProducer(PRODUCER_GROUP, getAclRpcHook(), true, null);
        producer.setNamesrvAddr(ENDPOINTS);
        producer.setAccessChannel(AccessChannel.CLOUD);
        producer.start();

        int successCount = 0;
        try {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = MESSAGE_PREFIX + i;
                Message msg = new Message(SOURCE_TOPIC_1, TAGS, "key_" + i, body.getBytes());
                SendResult result = producer.send(msg, 5000);
                System.out.printf(
                        "  Sent: msgId=%s, queue=%s-%d%n",
                        result.getMsgId(),
                        result.getMessageQueue().getBrokerName(),
                        result.getMessageQueue().getQueueId());
                successCount++;
            }
        } finally {
            producer.shutdown();
        }

        System.out.printf(
                "[Step 2] Done: %d/%d messages sent%n%n", successCount, MESSAGE_COUNT);
        if (successCount < MESSAGE_COUNT) {
            throw new RuntimeException(
                    "Failed to produce all messages: " + successCount + "/" + MESSAGE_COUNT);
        }
    }

    // ---- Step 4: Verify messages arrived in sink topic ----

    private void verifySinkMessages() throws Exception {
        System.out.println("[Step 4] Verifying messages in " + SINK_TOPIC_1);

        DefaultLitePullConsumer consumer =
                new DefaultLitePullConsumer(CONSUMER_GROUP, getAclRpcHook());
        consumer.setNamesrvAddr(ENDPOINTS);
        consumer.setAccessChannel(AccessChannel.CLOUD);
        consumer.setAutoCommit(false);
        consumer.setVipChannelEnabled(false);
        consumer.start();

        List<String> receivedMessages = new ArrayList<>();
        try {
            Collection<MessageQueue> queues = consumer.fetchMessageQueues(SINK_TOPIC_1);
            System.out.println("  Found " + queues.size() + " queues for " + SINK_TOPIC_1);
            consumer.assign(queues);
            for (MessageQueue mq : queues) {
                consumer.seekToBegin(mq);
            }

            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline
                    && receivedMessages.size() < MESSAGE_COUNT) {
                List<MessageExt> batch = consumer.poll(3000);
                for (MessageExt msg : batch) {
                    String body = new String(msg.getBody());
                    if (body.startsWith(MESSAGE_PREFIX)) {
                        receivedMessages.add(body);
                    }
                }
            }
        } finally {
            consumer.shutdown();
        }

        System.out.printf(
                "[Step 4] Verified: %d/%d messages received in sink topic%n",
                receivedMessages.size(), MESSAGE_COUNT);

        assertTrue(
                "Expected at least " + MESSAGE_COUNT + " messages in sink, got "
                        + receivedMessages.size(),
                receivedMessages.size() >= MESSAGE_COUNT);
    }

    // ---- Inner classes ----

    /** Deserializes the message body as a UTF-8 string. */
    private static class StringBodyDeserializationSchema
            implements RocketMQDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(MessageView messageView, Collector<String> out) {
            out.collect(new String(messageView.getBody()));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
