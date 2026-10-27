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

package org.apache.flink.connector.rocketmq.sink.committer;

import org.apache.flink.annotation.Internal;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageQueue;

/** The writer state for RocketMQ connector. We would use in RocketMQ committer. */
@Internal
public class SendCommittable {

    private String topic;

    private String brokerName;

    private Integer queueId;

    private Long queueOffset;

    private String msgId;

    private String offsetMsgId;

    private String transactionId;

    /**
     * Address of the node that accepted the half message, resolved from the writer's own publish
     * route right after the send. A proxy keeps the transaction context of a half message in its
     * own memory, so ending the transaction on any other node is discarded while still being
     * answered with a success code.
     */
    private String brokerAddress;

    public SendCommittable() {}

    public SendCommittable(SendResult sendResult) {
        this.topic = sendResult.getMessageQueue().getTopic();
        this.brokerName = sendResult.getMessageQueue().getBrokerName();
        this.queueId = sendResult.getMessageQueue().getQueueId();
        this.queueOffset = sendResult.getQueueOffset();
        this.msgId = sendResult.getMsgId();
        this.offsetMsgId = sendResult.getOffsetMsgId();
        this.transactionId = sendResult.getTransactionId();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getOffsetMsgId() {
        return offsetMsgId != null ? offsetMsgId : "";
    }

    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public MessageQueue getMessageQueue() {
        return new MessageQueue(topic, brokerName, queueId);
    }

    public long getMessageOffset() {
        try {
            if (StringUtils.isNotBlank(offsetMsgId)) {
                return MessageDecoder.decodeMessageId(offsetMsgId).getOffset();
            } else {
                return MessageDecoder.decodeMessageId(msgId).getOffset();
            }
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public String toString() {
        return "SendCommittable{"
                + "topic='"
                + topic
                + '\''
                + ", brokerName='"
                + brokerName
                + '\''
                + ", queueId="
                + queueId
                + ", queueOffset="
                + queueOffset
                + ", msgId='"
                + msgId
                + '\''
                + ", offsetMsgId='"
                + offsetMsgId
                + '\''
                + ", transactionId='"
                + transactionId
                + '\''
                + ", brokerAddress='"
                + brokerAddress
                + '\''
                + '}';
    }
}
