/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.grpc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.ack.AckableMessage;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQThrottleProcessFunction;
import org.apache.flink.connector.rocketmq.grpc.sink.RocketMQGrpcSink;
import org.apache.flink.connector.rocketmq.grpc.source.ConsumerMode;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the RocketMQ gRPC connector. They require a running RocketMQ 5.x instance
 * with the gRPC proxy enabled and are therefore skipped unless {@code ROCKETMQ_GRPC_ENDPOINTS} is
 * set; export it together with {@code ROCKETMQ_GRPC_ACCESS_KEY} and {@code
 * ROCKETMQ_GRPC_SECRET_KEY} to run them against a real cluster.
 */
@EnabledIfEnvironmentVariable(named = "ROCKETMQ_GRPC_ENDPOINTS", matches = ".+")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RocketMQGrpcITCase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQGrpcITCase.class);

    // -- Instance configuration (read from environment; never hard-code credentials) --
    private static final String ENDPOINTS = System.getenv("ROCKETMQ_GRPC_ENDPOINTS");
    private static final String ACCESS_KEY = System.getenv("ROCKETMQ_GRPC_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("ROCKETMQ_GRPC_SECRET_KEY");

    // -- Topics and groups --
    private static final String SINK_TOPIC = "flink-sink-1";
    private static final String SINK_LITE_TOPIC = "flink-sink-1-lite";
    private static final String SOURCE_TOPIC = "flink-source";
    private static final String E2E_SOURCE_TOPIC = "flink-source-1";
    private static final String E2E_SINK_TOPIC = "flink-sink-2";
    private static final String E2E_SINK_LITE_TOPIC = "flink-sink-2-lite";
    private static final String SIMPLE_SOURCE_TOPIC =
            System.getProperty("rocketmq.simple.topic", "flink-source-simple");
    private static final String CONSUMER_GROUP = "GID-flink";

    private static final int NUM_MESSAGES = 50;
    private static final long COLLECT_TIMEOUT_SECONDS = 120;

    /**
     * Each test run uses a unique prefix so that messages from previous runs can be distinguished
     * from the current run's messages.
     */
    private final String runId = UUID.randomUUID().toString().substring(0, 8);

    // -----------------------------------------------------------------------
    //  Test 1: Sink writes messages
    // -----------------------------------------------------------------------

    @Test
    @Order(1)
    void testSinkWritesMessages() throws Exception {
        final String prefix = "sink-" + runId + "-";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final List<String> messages = new ArrayList<>(NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            messages.add(prefix + i);
        }

        final RocketMQGrpcSink<String> sink =
                RocketMQGrpcSink.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setTopic(SINK_TOPIC)
                        .setLiteTopic(SINK_LITE_TOPIC)
                        .setConfig(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY)
                        .setValueOnlySerializer(new SimpleStringSchema())
                        .build();

        env.fromData(messages).sinkTo(sink);
        env.execute("RocketMQ gRPC Sink Test");
        LOG.info("Successfully wrote {} messages to topic {}", NUM_MESSAGES, SINK_TOPIC);
    }

    // -----------------------------------------------------------------------
    //  Test 2: Source reads messages
    // -----------------------------------------------------------------------

    @Test
    @Order(2)
    void testSourceReadsMessages() throws Exception {
        final String prefix = "src-" + runId + "-";

        // Seed messages using the RocketMQ Producer API directly.
        seedMessagesViaProducer(SOURCE_TOPIC, NUM_MESSAGES, prefix);

        // Read them back using the Flink source.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setMode(ConsumerMode.LITE)
                        .setMainTopic(SOURCE_TOPIC)
                        .setConfig(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RocketMQ gRPC Source")
                        .process(
                                new RocketMQThrottleProcessFunction<String>(
                                        ackConfiguration(), value -> Optional.empty()))
                        .returns(String.class);
        stream.addSink(new CollectingSinkFunction());

        // Collect messages, filtering by the current run's prefix.
        final List<String> results =
                CollectingSinkFunction.startAndCollect(env, NUM_MESSAGES, prefix);
        LOG.info("Collected {} messages (prefix={}) from source", results.size(), prefix);

        assertThat(results).hasSize(NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            assertThat(results).contains(prefix + i);
        }
    }

    // -----------------------------------------------------------------------
    //  Test 3: End-to-end pipeline (Source -> map -> Sink)
    // -----------------------------------------------------------------------

    @Test
    @Order(3)
    void testEndToEndPipeline() throws Exception {
        final String prefix = "e2e-" + runId + "-";

        // Seed messages into the e2e source topic.
        seedMessagesViaProducer(E2E_SOURCE_TOPIC, NUM_MESSAGES, prefix);

        // Build pipeline: Source reads from flink-source-1 -> uppercase -> Sink to flink-sink-2.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setMode(ConsumerMode.LITE)
                        .setMainTopic(E2E_SOURCE_TOPIC)
                        .setConfig(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final RocketMQGrpcSink<String> sink =
                RocketMQGrpcSink.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setTopic(E2E_SINK_TOPIC)
                        .setLiteTopic(E2E_SINK_LITE_TOPIC)
                        .setConfig(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY)
                        .setValueOnlySerializer(new SimpleStringSchema())
                        .build();

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RocketMQ gRPC Source")
                        .process(
                                new RocketMQThrottleProcessFunction<String>(
                                        ackConfiguration(), value -> Optional.empty()))
                        .returns(String.class);
        stream.map(String::toUpperCase).sinkTo(sink);

        // Run the pipeline in a background thread.
        final Thread jobThread =
                new Thread(
                        () -> {
                            try {
                                env.execute("E2E Pipeline");
                            } catch (Exception e) {
                                LOG.info("E2E pipeline ended: {}", e.getMessage());
                            }
                        });
        jobThread.setDaemon(true);
        jobThread.start();

        // Wait for the pipeline to process.
        Thread.sleep(30_000);

        env.close();
        jobThread.interrupt();
        jobThread.join(10_000);

        // Verify by reading from the sink topic, filtering by the uppercased prefix.
        final String upperPrefix = prefix.toUpperCase();
        final List<String> results =
                readMessagesViaConsumer(E2E_SINK_TOPIC, NUM_MESSAGES, upperPrefix);
        LOG.info(
                "Read {} messages from sink topic {} (prefix={})",
                results.size(),
                E2E_SINK_TOPIC,
                upperPrefix);

        assertThat(results).hasSize(NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            assertThat(results).contains(upperPrefix + i);
        }
    }

    // -----------------------------------------------------------------------
    //  Test: Simple mode consumes a normal topic with checkpoint-based ack
    // -----------------------------------------------------------------------

    @Test
    void testSimpleModeConsumesNormalTopic() throws Exception {
        final String prefix = "simple-" + runId + "-";

        seedMessagesViaProducer(SIMPLE_SOURCE_TOPIC, NUM_MESSAGES, prefix);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);

        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setMode(ConsumerMode.SIMPLE)
                        .setTopic(SIMPLE_SOURCE_TOPIC)
                        .setConfig(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY)
                        .setConfig(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "RocketMQ gRPC Source")
                        .map(AckableMessage::getValue)
                        .returns(String.class);
        stream.addSink(new CollectingSinkFunction());

        // Start the job in a daemon thread (same pattern as startAndCollect), but keep it
        // alive after enough records are collected: SIMPLE mode acks only after a checkpoint
        // completes, and closing the job immediately would leave nothing acked.
        CollectingSinkFunction.QUEUE.clear();
        final Thread jobThread =
                new Thread(
                        () -> {
                            try {
                                env.execute("Simple Mode Collecting Job");
                            } catch (Exception e) {
                                LOG.info(
                                        "Simple mode collecting job ended: {}", e.getMessage());
                            }
                        });
        jobThread.setDaemon(true);
        jobThread.start();

        // Collect messages matching the prefix until we have enough or timeout.
        final List<String> results = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + COLLECT_TIMEOUT_SECONDS * 1000;
        while (results.size() < NUM_MESSAGES && System.currentTimeMillis() < deadline) {
            final String item = CollectingSinkFunction.QUEUE.poll(500, TimeUnit.MILLISECONDS);
            if (item != null && item.startsWith(prefix)) {
                results.add(item);
            }
        }
        LOG.info("Collected {} messages (prefix={}) in simple mode", results.size(), prefix);

        assertThat(results).hasSize(NUM_MESSAGES);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            assertThat(results).contains(prefix + i);
        }

        // Keep the job running so that at least one checkpoint completes (interval is 5s)
        // and the reader acks the tracked receipt handles.
        Thread.sleep(20_000);

        env.close();
        jobThread.interrupt();
        jobThread.join(10_000);

        // Wait for the default 60s invisible duration to expire so that any un-acked message
        // would become visible for redelivery again.
        Thread.sleep(75_000);

        // Verify no un-acked messages remain: a fresh consumer under the same group must not
        // receive any message of this run's prefix.
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final ClientConfiguration clientConfig =
                ClientConfiguration.newBuilder()
                        .setEndpoints(ENDPOINTS)
                        .setCredentialProvider(
                                new StaticSessionCredentialsProvider(ACCESS_KEY, SECRET_KEY))
                        .build();

        try (SimpleConsumer consumer =
                provider.newSimpleConsumerBuilder()
                        .setClientConfiguration(clientConfig)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setAwaitDuration(Duration.ofSeconds(5))
                        .setSubscriptionExpressions(
                                Collections.singletonMap(
                                        SIMPLE_SOURCE_TOPIC, FilterExpression.SUB_ALL))
                        .build()) {
            int redelivered = 0;
            final List<MessageView> messages = consumer.receive(32, Duration.ofSeconds(20));
            for (MessageView msg : messages) {
                final ByteBuffer buf = msg.getBody();
                final byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                final String body = new String(bytes, StandardCharsets.UTF_8);
                if (body.startsWith(prefix)) {
                    redelivered++;
                }
            }
            assertThat(redelivered)
                    .withFailMessage(
                            "Expected no redelivered messages after checkpoint-aligned ack,"
                                    + " but got %d",
                            redelivered)
                    .isEqualTo(0);
        }
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------

    /**
     * Build a client {@link Configuration} for the downstream ack operators; credentials come from
     * the environment and never travel in the data stream.
     */
    private Configuration ackConfiguration() {
        final Configuration config = new Configuration();
        config.set(RocketMQGrpcOptions.ENDPOINTS, ENDPOINTS);
        config.set(RocketMQGrpcOptions.ACCESS_KEY, ACCESS_KEY);
        config.set(RocketMQGrpcOptions.SECRET_KEY, SECRET_KEY);
        return config;
    }

    /** Seed messages into the given topic using the RocketMQ gRPC Producer API directly. */
    private void seedMessagesViaProducer(String topic, int count, String prefix) throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final ClientConfiguration clientConfig =
                ClientConfiguration.newBuilder()
                        .setEndpoints(ENDPOINTS)
                        .setCredentialProvider(
                                new StaticSessionCredentialsProvider(ACCESS_KEY, SECRET_KEY))
                        .build();

        try (Producer producer =
                provider.newProducerBuilder()
                        .setClientConfiguration(clientConfig)
                        .setTopics(topic)
                        .build()) {
            for (int i = 0; i < count; i++) {
                final Message message =
                        provider.newMessageBuilder()
                                .setTopic(topic)
                                .setBody((prefix + i).getBytes(StandardCharsets.UTF_8))
                                .build();
                producer.send(message);
            }
        }
        LOG.info("Seeded {} messages into topic {} (prefix={})", count, topic, prefix);

        // Give the broker a moment to make messages available for consumers.
        Thread.sleep(3000);
    }

    /**
     * Read messages from a topic using the RocketMQ gRPC SimpleConsumer API directly, filtering by
     * the given prefix. Used to verify end-to-end pipeline output.
     */
    private List<String> readMessagesViaConsumer(String topic, int expectedCount, String prefix)
            throws Exception {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        final ClientConfiguration clientConfig =
                ClientConfiguration.newBuilder()
                        .setEndpoints(ENDPOINTS)
                        .setCredentialProvider(
                                new StaticSessionCredentialsProvider(ACCESS_KEY, SECRET_KEY))
                        .build();

        final List<String> results = new ArrayList<>();
        final org.apache.rocketmq.client.apis.consumer.FilterExpression filterExpression =
                new org.apache.rocketmq.client.apis.consumer.FilterExpression("*");
        final java.util.Map<String, org.apache.rocketmq.client.apis.consumer.FilterExpression>
                subscriptions = Collections.singletonMap(topic, filterExpression);

        try (org.apache.rocketmq.client.apis.consumer.SimpleConsumer consumer =
                provider.newSimpleConsumerBuilder()
                        .setClientConfiguration(clientConfig)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setAwaitDuration(Duration.ofSeconds(15))
                        .setSubscriptionExpressions(subscriptions)
                        .build()) {

            final long deadline = System.currentTimeMillis() + COLLECT_TIMEOUT_SECONDS * 1000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                final List<org.apache.rocketmq.client.apis.message.MessageView> messages =
                        consumer.receive(32, Duration.ofSeconds(15));
                for (org.apache.rocketmq.client.apis.message.MessageView msg : messages) {
                    final ByteBuffer buf = msg.getBody();
                    final byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    final String body = new String(bytes, StandardCharsets.UTF_8);
                    consumer.ack(msg);
                    if (body.startsWith(prefix)) {
                        results.add(body);
                    }
                }
            }
        }
        return results;
    }

    /**
     * A collecting sink function that gathers emitted records into a thread-safe queue. Used to
     * retrieve results from an unbounded source by cancelling the job once enough records matching
     * the prefix are collected.
     */
    @SuppressWarnings("deprecation")
    private static class CollectingSinkFunction
            extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<String> {

        private static final long serialVersionUID = 1L;

        private static final LinkedBlockingQueue<String> QUEUE = new LinkedBlockingQueue<>();

        @Override
        public void invoke(String value, Context context) {
            QUEUE.add(value);
        }

        /**
         * Execute the given environment and collect at least {@code expectedCount} records matching
         * the prefix. The job is cancelled once enough records are gathered or after a timeout.
         */
        static List<String> startAndCollect(
                StreamExecutionEnvironment env, int expectedCount, String prefix) throws Exception {
            QUEUE.clear();

            final Thread jobThread =
                    new Thread(
                            () -> {
                                try {
                                    env.execute("Collecting Job");
                                } catch (Exception e) {
                                    LOG.info("Collecting job ended: {}", e.getMessage());
                                }
                            });
            jobThread.setDaemon(true);
            jobThread.start();

            // Collect messages matching the prefix until we have enough or timeout.
            final List<String> results = new ArrayList<>();
            final long deadline = System.currentTimeMillis() + COLLECT_TIMEOUT_SECONDS * 1000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                final String item = QUEUE.poll(500, TimeUnit.MILLISECONDS);
                if (item != null && item.startsWith(prefix)) {
                    results.add(item);
                }
            }

            env.close();
            jobThread.interrupt();
            jobThread.join(10_000);

            // Drain remaining matching items.
            final List<String> remaining = new ArrayList<>();
            QUEUE.drainTo(remaining);
            for (String item : remaining) {
                if (item.startsWith(prefix)) {
                    results.add(item);
                }
            }
            return results;
        }
    }
}
