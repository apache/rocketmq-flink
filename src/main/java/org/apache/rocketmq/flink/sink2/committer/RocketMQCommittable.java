/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.flink.sink2.committer;

import java.util.Objects;

public class RocketMQCommittable {

    private final String producerGroup;

    private final String msgId;

    public String getProducerGroup() {
        return producerGroup;
    }

    public String getMsgId() {
        return msgId;
    }

    public RocketMQCommittable(String producerGroup, String msgId) {
        this.producerGroup = producerGroup;
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "RocketMQCommittable{"
                + "producerGroup='"
                + producerGroup
                + '\''
                + ", transactionId='"
                + msgId
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RocketMQCommittable that = (RocketMQCommittable) o;
        return Objects.equals(producerGroup, that.producerGroup)
                && Objects.equals(msgId, that.msgId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerGroup, msgId);
    }
}
