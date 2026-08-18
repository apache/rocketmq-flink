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

import org.apache.flink.connector.rocketmq.grpc.ack.RocketMQReceiptHandle;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CheckpointAckTracker}. */
class CheckpointAckTrackerTest {

    @Test
    void completeUpToReturnsSnapshottedHandlesTest() {
        final CheckpointAckTracker tracker = new CheckpointAckTracker();
        tracker.add(handle(1));
        tracker.add(handle(2));
        tracker.snapshot(7L);

        final List<RocketMQReceiptHandle> completed = tracker.completeUpTo(7L);
        assertThat(completed).containsExactly(handle(1), handle(2));
    }

    @Test
    void completeUpToKeepsLaterCheckpointsTest() {
        final CheckpointAckTracker tracker = new CheckpointAckTracker();
        tracker.add(handle(1));
        tracker.snapshot(7L);
        tracker.add(handle(2));
        tracker.snapshot(8L);

        assertThat(tracker.completeUpTo(7L)).containsExactly(handle(1));
        assertThat(tracker.completeUpTo(8L)).containsExactly(handle(2));
        assertThat(tracker.completeUpTo(9L)).isEmpty();
    }

    @Test
    void completeUpToMergesAllCheckpointsUpToIdTest() {
        final CheckpointAckTracker tracker = new CheckpointAckTracker();
        tracker.add(handle(1));
        tracker.snapshot(1L);
        tracker.add(handle(2));
        tracker.snapshot(2L);

        assertThat(tracker.completeUpTo(2L)).containsExactly(handle(1), handle(2));
    }

    @Test
    void handlesAddedAfterSnapshotGoToNextCheckpointTest() {
        final CheckpointAckTracker tracker = new CheckpointAckTracker();
        tracker.snapshot(1L);
        tracker.add(handle(1));

        assertThat(tracker.completeUpTo(1L)).isEmpty();
        tracker.snapshot(2L);
        assertThat(tracker.completeUpTo(2L)).containsExactly(handle(1));
    }

    @Test
    void emptySnapshotIsNotRecordedTest() {
        final CheckpointAckTracker tracker = new CheckpointAckTracker();
        tracker.snapshot(1L);
        tracker.add(handle(1));
        tracker.snapshot(2L);

        // Checkpoint 1 carried nothing; handle(1) belongs to checkpoint 2 only.
        assertThat(tracker.completeUpTo(1L)).isEmpty();
        assertThat(tracker.completeUpTo(2L)).containsExactly(handle(1));
    }

    private static RocketMQReceiptHandle handle(int id) {
        return new RocketMQReceiptHandle(
                "127.0.0.1:8080", "", "group", "topic", null, "msg-" + id, "receipt-" + id, 1);
    }
}
