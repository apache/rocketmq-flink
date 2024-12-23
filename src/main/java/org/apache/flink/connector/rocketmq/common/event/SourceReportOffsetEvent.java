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

public class SourceReportOffsetEvent {
    private String topic;
    private String broker;
    private int queueId;
    private long checkpoint = -1;

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public void setCheckpoint(long checkpoint) {
        this.checkpoint = checkpoint;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getBroker() {
        return broker;
    }

    public String getTopic() {
        return topic;
    }
}
