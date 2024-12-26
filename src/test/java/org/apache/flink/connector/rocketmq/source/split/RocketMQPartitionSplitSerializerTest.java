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

package org.apache.flink.connector.rocketmq.source.split;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Test for {@link RocketMQPartitionSplitSerializer}. */
public class RocketMQPartitionSplitSerializerTest {

    @Test
    public void testSerializePartitionSplit() throws IOException {
        RocketMQPartitionSplitSerializer serializer = new RocketMQPartitionSplitSerializer();
        RocketMQSourceSplit expected =
                new RocketMQSourceSplit("test-split-serialization", "taobaodaily", 256, 100, 300);
        RocketMQSourceSplit actual =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(expected));
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        RocketMQPartitionSplitSerializer serializer = new RocketMQPartitionSplitSerializer();
        RocketMQSourceSplit originalSplit = new RocketMQSourceSplit(
                "testTopic", "testBroker", 0, 100L, 200L, false);

        byte[] serialized = serializer.serialize(originalSplit);
        RocketMQSourceSplit deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);

        assertEquals(originalSplit.getTopic(), deserializedSplit.getTopic());
        assertEquals(originalSplit.getBrokerName(), deserializedSplit.getBrokerName());
        assertEquals(originalSplit.getQueueId(), deserializedSplit.getQueueId());
        assertEquals(originalSplit.getStartingOffset(), deserializedSplit.getStartingOffset());
        assertEquals(originalSplit.getStoppingOffset(), deserializedSplit.getStoppingOffset());
        assertEquals(originalSplit.getIsIncrease(), deserializedSplit.getIsIncrease());
    }

    @Test
    public void testDeserializeWithOldVersion() throws IOException {
        RocketMQPartitionSplitSerializer serializer = new RocketMQPartitionSplitSerializer();
        RocketMQSourceSplit originalSplit = new RocketMQSourceSplit(
                "testTopic", "testBroker", 0, 100L, 200L, false);

        byte[] serialized = serializer.serialize(originalSplit);
        RocketMQSourceSplit deserializedSplit = serializer.deserialize(1, serialized);

        assertEquals(originalSplit.getTopic(), deserializedSplit.getTopic());
        assertEquals(originalSplit.getBrokerName(), deserializedSplit.getBrokerName());
        assertEquals(originalSplit.getQueueId(), deserializedSplit.getQueueId());
        assertEquals(originalSplit.getStartingOffset(), deserializedSplit.getStartingOffset());
        assertEquals(originalSplit.getStoppingOffset(), deserializedSplit.getStoppingOffset());
        assertEquals(originalSplit.getIsIncrease(), deserializedSplit.getIsIncrease());
    }

    @Test
    public void testDeserializeWithOldVersion1() throws IOException {
        RocketMQPartitionSplitSerializer serializer = new RocketMQPartitionSplitSerializer();
        RocketMQSourceSplit originalSplit = new RocketMQSourceSplit(
                "testTopic", "testBroker", 0, 100L, 200L, false);

        byte[] serialized = serializer.serialize(originalSplit);
        RocketMQSourceSplit deserializedSplit = serializer.deserialize(0, serialized);

        assertEquals(originalSplit.getTopic(), deserializedSplit.getTopic());
        assertEquals(originalSplit.getBrokerName(), deserializedSplit.getBrokerName());
        assertEquals(originalSplit.getQueueId(), deserializedSplit.getQueueId());
        assertEquals(originalSplit.getStartingOffset(), deserializedSplit.getStartingOffset());
        assertEquals(originalSplit.getStoppingOffset(), deserializedSplit.getStoppingOffset());
        assertEquals(true, deserializedSplit.getIsIncrease());
    }
}
