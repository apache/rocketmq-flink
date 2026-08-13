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

package org.apache.flink.connector.rocketmq.grpc.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

/**
 * A placeholder {@link SourceSplit} for the RocketMQ gRPC Pop consumption model.
 *
 * <p>Unlike a pull-based connector, the Pop model performs message-level load balancing on the
 * broker side: every subtask subscribes to all configured topics and the broker distributes
 * messages across the consumer group. There is therefore no per-topic, per-queue or offset state to
 * distribute. This split carries no data; it merely exists because the FLIP-27 {@code Source} API
 * requires a split type, and assigning a single instance to a reader is what triggers that reader
 * to start consuming.
 */
public class RocketMQGrpcSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 2L;

    /** The single, constant split id used by every reader. */
    public static final String SPLIT_ID = "rocketmq-grpc-pop";

    public static final RocketMQGrpcSourceSplit INSTANCE = new RocketMQGrpcSourceSplit();

    public RocketMQGrpcSourceSplit() {}

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public String toString() {
        return "RocketMQGrpcSourceSplit(" + SPLIT_ID + ")";
    }

    @Override
    public int hashCode() {
        return SPLIT_ID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RocketMQGrpcSourceSplit;
    }
}
