package org.apache.flink.connector.rocketmq;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.connector.rocketmq.legacy.RocketMQSink;
import org.apache.flink.connector.rocketmq.legacy.RocketMQSourceFunction;
import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.legacy.common.serialization.SimpleKeyValueDeserializationSchema;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class main {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "staging-cnbj2-rocketmq.namesrv.api.xiaomi.net:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "test_pull_2");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "test_pull_2");

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "staging-cnbj2-rocketmq.namesrv.api.xiaomi.net:9876");

        RocketMQSourceFunction<Map<Object,Object>> source = new RocketMQSourceFunction(
                new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps);
        // use group offsets.
        // If there is no committed offset,consumer would start from the latest offset.
        source.setStartFromGroupOffsets(OffsetResetStrategy.LATEST);
        env.addSource(source)
                .name("rocketmq-source")
                .setParallelism(2)
                .process(new ProcessFunction<Map<Object, Object>, Map<Object, Object>>() {
                    @Override
                    public void processElement(
                            Map<Object, Object> in,
                            Context ctx,
                            Collector<Map<Object, Object>> out) {
                        HashMap result = new HashMap();
                        result.put("id", in.get("id"));
                        String[] arr = in.get("address").toString().split("\\s+");
                        result.put("province", arr[arr.length - 1]);
                        out.collect(result);
                    }
                })
                .name("upper-processor")
                .setParallelism(2)
                .process(new ProcessFunction<Map<Object, Object>, Message>() {
                    @Override
                    public void processElement(Map<Object, Object> value, Context ctx, Collector<Message> out) {
                        String jsonString = JSONObject.toJSONString(value);
                        Message message =
                                new Message(
                                        "flink-sink2",
                                        "",
                                        jsonString.getBytes(StandardCharsets.UTF_8));
                        out.collect(message);
                    }
                })
                .addSink(new RocketMQSink(producerProps).withBatchFlushOnCheckpoint(true))
                .name("rocketmq-sink")
                .setParallelism(2);

        try {
            env.execute("rocketmq-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
