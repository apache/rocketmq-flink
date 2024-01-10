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

package org.apache.flink.connector.rocketmq.legacy.sourceFunction;

import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.connector.rocketmq.legacy.RocketMQSourceFunction;
import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;
import org.apache.flink.connector.rocketmq.legacy.common.config.StartupMode;
import org.apache.flink.connector.rocketmq.legacy.common.serialization.SimpleStringDeserializationSchema;
import org.apache.flink.connector.rocketmq.legacy.common.util.TestUtils;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/** Tests for {@link RocketMQSourceFunction}. */
public class RocketMQSourceFunctionTest {

    @Test
    public void testSetStartupMode() throws NoSuchFieldException, IllegalAccessException {
        RocketMQSourceFunction<String> source =
                new RocketMQSourceFunction<>(
                        new SimpleStringDeserializationSchema(), new Properties());
        Assert.assertEquals(
                StartupMode.GROUP_OFFSETS, TestUtils.getFieldValue(source, "startMode"));
        source.setStartFromEarliest();
        Assert.assertEquals(StartupMode.EARLIEST, TestUtils.getFieldValue(source, "startMode"));
        source.setStartFromLatest();
        Assert.assertEquals(StartupMode.LATEST, TestUtils.getFieldValue(source, "startMode"));
        source.setStartFromTimeStamp(0L);
        Assert.assertEquals(StartupMode.TIMESTAMP, TestUtils.getFieldValue(source, "startMode"));
        source.setStartFromSpecificOffsets(null);
        Assert.assertEquals(
                StartupMode.SPECIFIC_OFFSETS, TestUtils.getFieldValue(source, "startMode"));
        source.setStartFromGroupOffsets();
        Assert.assertEquals(
                StartupMode.GROUP_OFFSETS, TestUtils.getFieldValue(source, "startMode"));
        Assert.assertEquals(
                OffsetResetStrategy.LATEST, TestUtils.getFieldValue(source, "offsetResetStrategy"));
        source.setStartFromGroupOffsets(OffsetResetStrategy.EARLIEST);
        Assert.assertEquals(
                OffsetResetStrategy.EARLIEST,
                TestUtils.getFieldValue(source, "offsetResetStrategy"));
    }

    @Test
    public void testRestartFromCheckpoint() throws Exception {
        DefaultLitePullConsumer consumer = Mockito.mock(DefaultLitePullConsumer.class);
        Mockito.when(consumer.committed(Mockito.any())).thenReturn(40L);
        Properties properties = new Properties();
        properties.setProperty(RocketMQConfig.CONSUMER_GROUP, "${ConsumerGroup}");
        properties.setProperty(RocketMQConfig.CONSUMER_TOPIC, "${SourceTopic}");
        properties.setProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);
        RocketMQSourceFunction<String> source =
                new RocketMQSourceFunction<>(new SimpleStringDeserializationSchema(), properties);
        source.setStartFromLatest();
        TestUtils.setFieldValue(source, "restored", true);
        HashMap<MessageQueue, Long> map = new HashMap<>();
        map.put(new MessageQueue("tpc", "broker-0", 0), 20L);
        map.put(new MessageQueue("tpc", "broker-0", 1), 21L);
        map.put(new MessageQueue("tpc", "broker-1", 0), 30L);
        map.put(new MessageQueue("tpc", "broker-1", 1), 31L);
        List<MessageQueue> allocateMessageQueues = new ArrayList<>(map.keySet());
        MessageQueue newMessageQueue = new MessageQueue("tpc", "broker-2", 0);
        allocateMessageQueues.add(newMessageQueue);
        TestUtils.setFieldValue(source, "messageQueues", allocateMessageQueues);
        TestUtils.setFieldValue(source, "consumer", consumer);
        TestUtils.setFieldValue(source, "restoredOffsets", map);
        TestUtils.setFieldValue(source, "offsetTable", new ConcurrentHashMap<>());
        source.initOffsetTableFromRestoredOffsets(allocateMessageQueues);
        Map<MessageQueue, Long> offsetTable = (Map) TestUtils.getFieldValue(source, "offsetTable");
        for (Map.Entry<MessageQueue, Long> entry : map.entrySet()) {
            assertEquals(offsetTable.containsKey(entry.getKey()), true);
            assertEquals(offsetTable.containsValue(entry.getValue()), true);
        }
        assertEquals(offsetTable.containsKey(newMessageQueue), true);
    }
}
