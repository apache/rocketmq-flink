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
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQThrottleProcessFunction;
import org.apache.flink.connector.rocketmq.grpc.sink.RocketMQGrpcSink;
import org.apache.flink.connector.rocketmq.grpc.source.InvisibleDurationRenewalPolicy;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSource;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions;
import org.apache.flink.connector.rocketmq.grpc.source.reader.MessageView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manual end-to-end verification of the RocketMQ gRPC connector against a live cluster (public
 * proxy SLB). Run with {@code mvn test-compile exec:java}. Phases:
 *
 * <ol>
 *   <li>Sink: publish lite messages to the parent topic.
 *   <li>Source + throttle: consume them; the throttle policy defers selected messages once by 10s
 *       (changeInvisibleDuration), verifying the throttle really delays re-delivery.
 *   <li>Renewal: a slow pipeline keeps messages queued in the split reader beyond the renewal
 *       trigger point, verifying the invisible-duration renewal fires and prevents duplicates.
 * </ol>
 */
public class RocketMQGrpcLiteE2EVerify {

    private static final String ENDPOINTS =
            System.getProperty("rocketmq.endpoints", "47.98.110.140:8081");
    private static final String MAIN_TOPIC = "LiteTest";
    private static final String CONSUMER_GROUP = "LiteSimpleConsumerGroup";

    private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);

    // value -> arrival timestamps (ms)
    private static final Map<String, List<Long>> ARRIVALS = new ConcurrentHashMap<>();
    // values the throttle policy has already deferred once
    private static final Map<String, Boolean> THROTTLED_ONCE = new ConcurrentHashMap<>();
    // renewal invocations observed by the renewal policy
    private static final AtomicInteger RENEWALS = new AtomicInteger();

    public static void main(String[] args) {
        // Run each phase in its own JVM: a finished local job cannot be cancelled cleanly, and a
        // lingering same-group consumer would steal the next phase's messages.
        final String phase = args.length > 0 ? args[0] : "phase1";
        System.out.println("=== RocketMQGrpcLiteE2EVerify " + phase + " runId=" + RUN_ID + " ===");
        try {
            if ("phase2".equals(phase)) {
                phase2Renewal();
            } else {
                phase1SinkAndThrottle();
            }
            System.out.println("=== " + phase + " PASSED ===");
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    // ------------------------------------------------------------------
    // Phase 1: sink writes; source + throttle policy verification
    // ------------------------------------------------------------------

    private static void phase1SinkAndThrottle() throws Exception {
        final String normalPrefix = "n-" + RUN_ID + "-";
        final String throttlePrefix = "thr-" + RUN_ID + "-";
        final List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(normalPrefix + i);
        }
        for (int i = 0; i < 5; i++) {
            messages.add(throttlePrefix + i);
        }
        runSinkJob(messages, "flink-e2e-lite");
        System.out.println("[phase1] sink wrote " + messages.size() + " lite messages");

        final StreamExecutionEnvironment env = localEnv();
        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setMainTopic(MAIN_TOPIC)
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final Configuration ackConfig = new Configuration();
        ackConfig.set(RocketMQGrpcOptions.ENDPOINTS, ENDPOINTS);

        final String tp = throttlePrefix;
        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "grpc-source")
                        .process(
                                new RocketMQThrottleProcessFunction<String>(
                                        ackConfig,
                                        value -> {
                                            if (value.startsWith(tp)
                                                    && THROTTLED_ONCE.putIfAbsent(value, true)
                                                            == null) {
                                                return Optional.of(Duration.ofSeconds(10));
                                            }
                                            return Optional.empty();
                                        }))
                        .returns(String.class);
        stream.map(RocketMQGrpcLiteE2EVerify::record);

        ARRIVALS.clear();
        runJobUntil(
                env,
                180,
                () -> {
                    for (int i = 0; i < 10; i++) {
                        if (count(normalPrefix + i) < 1) {
                            return false;
                        }
                    }
                    for (int i = 0; i < 5; i++) {
                        if (count(tp + i) < 2) {
                            return false;
                        }
                    }
                    return true;
                });

        for (int i = 0; i < 5; i++) {
            final List<Long> times = ARRIVALS.get(tp + i);
            check(times != null && times.size() >= 2, "throttled message not redelivered: " + i);
            final long gapMs = times.get(1) - times.get(0);
            check(gapMs >= 8000, "throttle gap too small: " + gapMs + "ms for " + tp + i);
            System.out.println("[phase1] " + tp + i + " redelivered after " + gapMs + " ms");
        }
        System.out.println("[phase1] PASS: send/receive + throttle (defer via invisible duration)");
    }

    // ------------------------------------------------------------------
    // Phase 2: invisible-duration renewal under backpressure
    // ------------------------------------------------------------------

    private static void phase2Renewal() throws Exception {
        final String prefix = "rnw-" + RUN_ID + "-";
        final int total = 60;
        // Spread across lite topics: the broker paces event dispatch per lite topic when un-acked
        // messages accumulate, which would starve the backpressure scenario otherwise.
        final StreamExecutionEnvironment sinkEnv = localEnv();
        for (int t = 0; t < 20; t++) {
            final List<String> messages = new ArrayList<>();
            for (int i = t; i < total; i += 20) {
                messages.add(prefix + i);
            }
            final RocketMQGrpcSink<String> sink =
                    RocketMQGrpcSink.<String>builder()
                            .setEndpoints(ENDPOINTS)
                            .setTopic(MAIN_TOPIC)
                            .setLiteTopic("flink-renew-lite-" + t)
                            .setValueOnlySerializer(new SimpleStringSchema())
                            .build();
            sinkEnv.fromData(messages).sinkTo(sink);
        }
        sinkEnv.execute("lite-sink-renew");
        System.out.println("[phase2] sink wrote " + total + " lite messages");

        final StreamExecutionEnvironment env = localEnv();
        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setMainTopic(MAIN_TOPIC)
                        .setConfig(
                                RocketMQGrpcSourceOptions.INVISIBLE_DURATION,
                                Duration.ofSeconds(20))
                        // Small batches so the downstream sleep keeps messages queued inside the
                        // split reader long enough to reach the renewal trigger point (20s - 18s).
                        .setConfig(RocketMQGrpcSourceOptions.MAX_MESSAGE_NUM, 4)
                        .setFetchConcurrency(4)
                        .setRenewalPolicyClass(CountingRenewalPolicy.class.getName())
                        .setRenewalAheadTime(Duration.ofSeconds(18))
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final Configuration ackConfig = new Configuration();
        ackConfig.set(RocketMQGrpcOptions.ENDPOINTS, ENDPOINTS);

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "grpc-source")
                        .process(
                                new RocketMQThrottleProcessFunction<String>(
                                        ackConfig, value -> Optional.empty()))
                        .returns(String.class);
        stream.map(
                value -> {
                    Thread.sleep(300); // backpressure so messages linger in the split reader
                    return record(value);
                });

        ARRIVALS.clear();
        RENEWALS.set(0);
        runJobUntil(
                env,
                240,
                () -> {
                    for (int i = 0; i < total; i++) {
                        if (count(prefix + i) < 1) {
                            return false;
                        }
                    }
                    return true;
                });

        int duplicates = 0;
        for (int i = 0; i < total; i++) {
            if (count(prefix + i) > 1) {
                duplicates++;
            }
        }
        System.out.println(
                "[phase2] renewals=" + RENEWALS.get() + ", duplicates=" + duplicates + "/" + total);
        check(RENEWALS.get() > 0, "renewal policy never triggered");
        check(duplicates == 0, "unexpected duplicates: " + duplicates);
        System.out.println("[phase2] PASS: invisible-duration renewal fired, no redelivery");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Renewal policy that counts invocations and extends by another 20s. */
    public static class CountingRenewalPolicy implements InvisibleDurationRenewalPolicy {
        private static final long serialVersionUID = 1L;

        @Override
        @Nullable
        public Duration renew(MessageView messageView, int renewalCount) {
            RENEWALS.incrementAndGet();
            return Duration.ofSeconds(20);
        }
    }

    private static void runSinkJob(List<String> messages, String liteTopic) throws Exception {
        final StreamExecutionEnvironment env = localEnv();
        final RocketMQGrpcSink<String> sink =
                RocketMQGrpcSink.<String>builder()
                        .setEndpoints(ENDPOINTS)
                        .setTopic(MAIN_TOPIC)
                        .setLiteTopic(liteTopic)
                        .setValueOnlySerializer(new SimpleStringSchema())
                        .build();
        env.fromData(messages).sinkTo(sink);
        env.execute("lite-sink-" + liteTopic);
    }

    private static StreamExecutionEnvironment localEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setParallelism(1);
        return env;
    }

    private static String record(String value) {
        ARRIVALS.computeIfAbsent(value, k -> new CopyOnWriteArrayList<>())
                .add(System.currentTimeMillis());
        return value;
    }

    private static int count(String value) {
        final List<Long> times = ARRIVALS.get(value);
        return times == null ? 0 : times.size();
    }

    private static void runJobUntil(
            StreamExecutionEnvironment env, int timeoutSeconds, Condition condition)
            throws Exception {
        final Thread jobThread =
                new Thread(
                        () -> {
                            try {
                                env.execute("verify-job");
                            } catch (Exception e) {
                                System.out.println("job ended: " + e.getMessage());
                            }
                        });
        jobThread.setDaemon(true);
        jobThread.start();

        final long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        boolean satisfied = false;
        int ticks = 0;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test()) {
                satisfied = true;
                break;
            }
            if (++ticks % 15 == 0) {
                System.out.println(
                        "[progress] distinct=" + ARRIVALS.size() + " after " + ticks + "s");
            }
            Thread.sleep(1000);
        }
        // Give in-flight acks a moment before tearing the job down.
        Thread.sleep(3000);
        env.close();
        jobThread.interrupt();
        jobThread.join(15000);
        check(satisfied, "condition not satisfied within " + timeoutSeconds + "s");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("VERIFY FAILED: " + message);
        }
    }

    @FunctionalInterface
    private interface Condition {
        boolean test();
    }
}
