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

package org.apache.flink.connector.rocketmq.grpc.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Tracks the receipt handles of emitted-but-unacknowledged messages of the SIMPLE mode, grouped by
 * the checkpoint that observed them. Handles snapshotted with checkpoint N are acknowledged once
 * checkpoint N completes; a failure before completion simply leaves the messages un-acked so the
 * broker redelivers them after their invisible duration (at-least-once).
 *
 * <p>The tracker holds no state that must survive failures: losing pending handles only causes
 * redelivery, never loss. All methods are synchronized because the record emitter (reader thread)
 * and the checkpoint callbacks (mailbox thread) may touch it concurrently.
 */
@Internal
public class CheckpointAckTracker {

    private final List<RocketMQReceiptHandle> pending = new ArrayList<>();
    private final TreeMap<Long, List<RocketMQReceiptHandle>> snapshotted = new TreeMap<>();

    /** Track the handle of an emitted message. */
    public synchronized void add(RocketMQReceiptHandle handle) {
        pending.add(handle);
    }

    /** Associate all handles emitted so far with the given checkpoint. */
    public synchronized void snapshot(long checkpointId) {
        if (!pending.isEmpty()) {
            snapshotted.put(checkpointId, new ArrayList<>(pending));
            pending.clear();
        }
    }

    /**
     * Return every handle whose checkpoint id is at most the completed one, removing them from the
     * tracker.
     */
    public synchronized List<RocketMQReceiptHandle> completeUpTo(long checkpointId) {
        final List<RocketMQReceiptHandle> completed = new ArrayList<>();
        while (!snapshotted.isEmpty() && snapshotted.firstKey() <= checkpointId) {
            completed.addAll(snapshotted.pollFirstEntry().getValue());
        }
        return completed;
    }
}
