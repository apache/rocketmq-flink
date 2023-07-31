package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.example.ConnectorConfig;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class RocketMQSourceTest {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceTest.class);

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        String directory = "flink-connector-rocketmq";
        String userHome = System.getProperty("user.home");
        String ckptPath = Paths.get(userHome, directory, "ckpt").toString();
        String sinkPath = Paths.get(userHome, directory, "sink").toString();
        String ckptUri = "file://" + File.separator + ckptPath;

        log.info("Connector checkpoint path: {}", ckptPath);

        RocketMQSource<String> source =
                RocketMQSource.<String>builder()
                        .setEndpoints(ConnectorConfig.ENDPOINTS)
                        .setConfig(RocketMQOptions.OPTIONAL_ACCESS_KEY, ConnectorConfig.ACCESS_KEY)
                        .setConfig(RocketMQOptions.OPTIONAL_SECRET_KEY, ConnectorConfig.SECRET_KEY)
                        .setGroupId(ConnectorConfig.CONSUMER_GROUP)
                        .setTopics(ConnectorConfig.SOURCE_TOPIC_1, ConnectorConfig.SOURCE_TOPIC_2)
                        .setMinOffsets(OffsetsSelector.earliest())
                        .setDeserializer(
                                new RocketMQDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            MessageView messageView, Collector<String> out) {
                                        out.collect(messageView.getMessageId());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .build();

        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.BIND_PORT.key(), 8088);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
        env.setStateBackend(new FsStateBackend(ckptUri));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(1));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        DataStreamSource<String> dataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "rocketmq-test-source")
                        .setParallelism(2);

        dataStream
                .setParallelism(1)
                .writeAsText(sinkPath, FileSystem.WriteMode.OVERWRITE)
                .name("rocketmq-test-sink")
                .setParallelism(2);

        env.execute("rocketmq-local-test");
        log.info("Start rocketmq source for test success");
    }
}
