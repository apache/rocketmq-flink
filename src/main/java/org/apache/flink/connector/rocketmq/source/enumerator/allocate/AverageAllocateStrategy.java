package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AverageAllocateStrategy implements AllocateStrategy {
    @Override
    public String getStrategyName() {
        return AllocateStrategyFactory.STRATEGY_NAME_AVERAGE;
    }

    @Override
    public Map<Integer, Set<RocketMQSourceSplit>> allocate(
            Collection<RocketMQSourceSplit> mqAll, int parallelism) {
        return null;
    }

    @Override
    public Map<Integer, Set<RocketMQSourceSplit>> allocate(
            Collection<RocketMQSourceSplit> mqAll, int parallelism, int globalAssignedNumber) {
        Map<Integer, Set<RocketMQSourceSplit>> result = new HashMap<>();
        for (RocketMQSourceSplit mq : mqAll) {
            int readerIndex =
                    this.getSplitOwner(mq.getTopic(), globalAssignedNumber++, parallelism);
            result.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(mq);
        }
        return result;
    }

    private int getSplitOwner(String topic, int partition, int numReaders) {
        int startIndex = ((topic.hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of RocketMQ partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + partition) % numReaders;
    }
}
