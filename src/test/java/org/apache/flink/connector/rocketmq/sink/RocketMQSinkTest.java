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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.rocketmq.example.ConnectorConfig;
import org.apache.flink.connector.rocketmq.sink.writer.serializer.RocketMQSerializationSchema;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceTest;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RocketMQSinkTest {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceTest.class);

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        String directory = "flink-connector-rocketmq";
        String userHome = System.getProperty("user.home");
        String ckptPath = Paths.get(userHome, directory, "ckpt").toString();
        String sinkPath = Paths.get(userHome, directory, "sink").toString();
        String ckptUri = "file://" + File.separator + ckptPath;

        log.info("Connector checkpoint path: {}", ckptPath);

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.BIND_PORT.key(), 8088);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStream<String> source =
                env.addSource(
                                new DataGeneratorSource<>(
                                        new RandomGenerator<String>() {
                                            @Override
                                            public String next() {
                                                long timestamp =
                                                        System.currentTimeMillis()
                                                                + random.nextInt(
                                                                        -60 * 1000, 60 * 1000);
                                                return String.join(
                                                        "|",
                                                        String.valueOf(random.nextInt(0, 1)),
                                                        String.valueOf(timestamp));
                                            }
                                        },
                                        1,
                                        10000L))
                        .returns(Types.STRING)
                        .setParallelism(2)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                                                Duration.ofMinutes(1))
                                        .withTimestampAssigner(
                                                (SerializableTimestampAssigner<String>)
                                                        (element, recordTimestamp) ->
                                                                Long.parseLong(
                                                                        element.split("\\|")[1]))
                                        .withIdleness(Duration.ofMinutes(1)))
                        .returns(Types.STRING)
                        .setParallelism(2);

        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
        env.setStateBackend(new FsStateBackend(ckptUri));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(1));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        source.writeAsText(sinkPath, FileSystem.WriteMode.OVERWRITE)
                .name("rocketmq-test-sink")
                .setParallelism(2);

        RocketMQSink<String> rocketmqSink =
                RocketMQSink.<String>builder()
                        .setEndpoints(ConnectorConfig.ENDPOINTS)
                        .setGroupId(ConnectorConfig.PRODUCER_GROUP)
                        .setConfig(
                                RocketMQSinkOptions.OPTIONAL_ACCESS_KEY, ConnectorConfig.ACCESS_KEY)
                        .setConfig(
                                RocketMQSinkOptions.OPTIONAL_SECRET_KEY, ConnectorConfig.SECRET_KEY)
                        .setSerializer(
                                (RocketMQSerializationSchema<String>)
                                        (element, context, timestamp) -> {
                                            String topic =
                                                    Long.parseLong(element.split("\\|")[1]) % 2 != 0
                                                            ? ConnectorConfig.SINK_TOPIC_1
                                                            : ConnectorConfig.SINK_TOPIC_2;
                                            return new Message(
                                                    topic,
                                                    ConnectorConfig.TAGS,
                                                    element.getBytes());
                                        })
                        // .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        // If you use transaction message, need set transaction timeout
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setConfig(
                                RocketMQSinkOptions.TRANSACTION_TIMEOUT,
                                TimeUnit.SECONDS.toSeconds(30))
                        .build();

        source.sinkTo(rocketmqSink).setParallelism(2);

        env.execute("rocketmq-local-test");
        log.info("Start rocketmq sink for test success");
    }
}
