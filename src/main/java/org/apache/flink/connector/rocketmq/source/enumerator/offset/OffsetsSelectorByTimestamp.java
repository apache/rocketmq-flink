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

package org.apache.flink.connector.rocketmq.source.enumerator.offset;

import org.apache.flink.connector.rocketmq.legacy.common.config.OffsetResetStrategy;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class OffsetsSelectorByTimestamp implements OffsetsSelector {
    private static final long serialVersionUID = 2932230571773627233L;
    private final long startingTimestamp;

    OffsetsSelectorByTimestamp(long startingTimestamp) {
        this.startingTimestamp = startingTimestamp;
    }

    @Override
    public Map<MessageQueue, Long> getMessageQueueOffsets(
            Collection<MessageQueue> messageQueues, MessageQueueOffsetsRetriever offsetsRetriever) {
        Map<MessageQueue, Long> startingTimestamps = new HashMap<>();
        Map<MessageQueue, Long> initialOffsets = new HashMap<>();

        // First get the current end offsets of the partitions. This is going to be used
        // in case we cannot find a suitable offsets based on the timestamp, i.e. the message
        // meeting the requirement of the timestamp have not been produced to RocketMQ yet,
        // in this case, we just use the latest offset.
        // We need to get the latest offsets before querying offsets by time to ensure that
        // no message is going to be missed.
        Map<MessageQueue, Long> endOffsets = offsetsRetriever.maxOffsets(messageQueues);
        messageQueues.forEach(tp -> startingTimestamps.put(tp, startingTimestamp));
        offsetsRetriever
                .offsetsForTimes(startingTimestamps)
                .forEach(
                        (mq, offsetByTimestamp) -> {
                            long offset =
                                    offsetByTimestamp != null
                                            ? offsetByTimestamp
                                            : endOffsets.get(mq);
                            initialOffsets.put(mq, offset);
                        });
        return initialOffsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return OffsetResetStrategy.LATEST;
    }
}
