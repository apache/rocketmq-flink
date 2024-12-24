package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class AverageAllocateStrategyTest {

    private static final String BROKER_NAME = "brokerName";
    private static final String PREFIX_TOPIC = "test-topic-";
    private static final int NUM_SPLITS = 900;
    private static final int[] SPLIT_SIZE = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000};

    @Test
    public void averageAllocateStrategyTest() {
        AllocateStrategy allocateStrategy = new AverageAllocateStrategy();
        Collection<RocketMQSourceSplit> mqAll = new ArrayList<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQSourceSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i % SPLIT_SIZE.length]));
        }
        int parallelism = 3;
        Map<Integer, Set<RocketMQSourceSplit>> result =
                allocateStrategy.allocate(mqAll, parallelism, 0);
        assertEquals(NUM_SPLITS / parallelism, result.get(0).size());
        assertEquals(NUM_SPLITS / parallelism, result.get(1).size());
        assertEquals(NUM_SPLITS / parallelism, result.get(2).size());
    }

    @Test
    public void averagesAllocateStrategyTest() {
        AllocateStrategy allocateStrategy = new AverageAllocateStrategy();
        Collection<RocketMQSourceSplit> mqAll = new ArrayList<>();
        for (int i = 0; i < NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQSourceSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i % SPLIT_SIZE.length]));
        }
        int parallelism = 3;
        Map<Integer, Set<RocketMQSourceSplit>> result =
                allocateStrategy.allocate(mqAll, parallelism, 0);
        assertEquals(NUM_SPLITS / parallelism, result.get(0).size());
        assertEquals(NUM_SPLITS / parallelism, result.get(1).size());
        assertEquals(NUM_SPLITS / parallelism, result.get(2).size());

        mqAll.clear();
        for (int i = NUM_SPLITS; i < 8 + NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQSourceSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i % SPLIT_SIZE.length]));
        }
        Map<Integer, Set<RocketMQSourceSplit>> result1 =
                allocateStrategy.allocate(mqAll, parallelism, NUM_SPLITS);

        mqAll.clear();
        for (int i = 8 + NUM_SPLITS; i < 8 + 7 + NUM_SPLITS; i++) {
            mqAll.add(
                    new RocketMQSourceSplit(
                            PREFIX_TOPIC + (i + 1), BROKER_NAME, i, 0, SPLIT_SIZE[i % SPLIT_SIZE.length]));
        }
        Map<Integer, Set<RocketMQSourceSplit>> result2 =
                allocateStrategy.allocate(mqAll, parallelism, NUM_SPLITS + 8);

        result1.forEach((k, v) -> result.computeIfAbsent(k, r -> new HashSet<>()).addAll(v));
        result2.forEach((k, v) -> result.computeIfAbsent(k, r -> new HashSet<>()).addAll(v));

        // No matter how many times it's assigned, it's always equal
        assertEquals((NUM_SPLITS + 8 + 7) / parallelism, result.get(0).size());
        assertEquals((NUM_SPLITS + 8 + 7) / parallelism, result.get(1).size());
        assertEquals((NUM_SPLITS + 8 + 7) / parallelism, result.get(2).size());
    }
}
