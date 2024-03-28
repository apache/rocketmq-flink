package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BroadcastAllocateStrategyTest {

    private static final String BROKER_NAME = "brokerName";
    private static final String PREFIX_TOPIC = "test-topic-";
    private static final int NUM_SPLITS = 3;
    private static final int[] SPLIT_SIZE = {1000, 2000, 3000};

    @Test
    public void broadcastAllocateStrategyTest() {
        AllocateStrategy allocateStrategy = new BroadcastAllocateStrategy();
        Collection<RocketMQSourceSplit> mqAll = new ArrayList<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQSourceSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i]));
        }
        int parallelism = 3;
        Map<Integer, Set<RocketMQSourceSplit>> result =
                allocateStrategy.allocate(mqAll, parallelism);
        assertEquals(parallelism, result.size());
        for (int i = 0; i < parallelism; i++) {
            Set<RocketMQSourceSplit> splits = result.get(i);
            assertEquals(NUM_SPLITS, splits.size());

            for (int j = 0; j < NUM_SPLITS; j++) {
                assertTrue(
                        splits.contains(
                                new RocketMQSourceSplit(
                                        PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i])));
            }
        }
    }
}
