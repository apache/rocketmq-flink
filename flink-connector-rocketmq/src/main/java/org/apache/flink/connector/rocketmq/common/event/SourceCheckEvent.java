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

package org.apache.flink.connector.rocketmq.common.event;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Map;

public class SourceCheckEvent implements SourceEvent {
    private Map<MessageQueue, Tuple2<Long, Long>> assignedMq;

    public Map<MessageQueue, Tuple2<Long, Long>> getAssignedMq() {
        return assignedMq;
    }

    public void setAssignedMq(Map<MessageQueue, Tuple2<Long, Long>> assignedMq) {
        this.assignedMq = assignedMq;
    }
}
