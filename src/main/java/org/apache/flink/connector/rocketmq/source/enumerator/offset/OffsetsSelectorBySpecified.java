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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OffsetsSelectorBySpecified implements OffsetsSelector, OffsetsValidator {

    private final Map<MessageQueue, Long> initialOffsets;
    private final OffsetResetStrategy offsetResetStrategy;

    OffsetsSelectorBySpecified(
            Map<MessageQueue, Long> initialOffsets, OffsetResetStrategy offsetResetStrategy) {
        this.initialOffsets = Collections.unmodifiableMap(initialOffsets);
        this.offsetResetStrategy = offsetResetStrategy;
    }

    @Override
    public Map<MessageQueue, Long> getMessageQueueOffsets(
            Collection<MessageQueue> messageQueues, MessageQueueOffsetsRetriever offsetsRetriever) {
        Map<MessageQueue, Long> offsets = new HashMap<>();
        List<MessageQueue> toLookup = new ArrayList<>();
        for (MessageQueue tp : messageQueues) {
            Long offset = initialOffsets.get(tp);
            if (offset == null) {
                toLookup.add(tp);
            } else {
                offsets.put(tp, offset);
            }
        }
        if (!toLookup.isEmpty()) {
            // First check the committed offsets.
            Map<MessageQueue, Long> committedOffsets = offsetsRetriever.committedOffsets(toLookup);
            offsets.putAll(committedOffsets);
            toLookup.removeAll(committedOffsets.keySet());

            switch (offsetResetStrategy) {
                case EARLIEST:
                    offsets.putAll(offsetsRetriever.minOffsets(toLookup));
                    break;
                case LATEST:
                    offsets.putAll(offsetsRetriever.maxOffsets(toLookup));
                    break;
                default:
                    throw new IllegalStateException(
                            "Cannot find initial offsets for partitions: " + toLookup);
            }
        }
        return offsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public void validate(Properties rocketmqSourceProperties) {}
}
