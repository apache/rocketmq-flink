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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class MessageViewExt implements MessageView {

    private static final String KEY_SEPARATE = "";

    private final String messageId;

    private final String topic;

    private final String brokerName;

    private final int queueId;

    private final long queueOffset;

    private final int storeSize;

    private final String tag;

    private final Collection<String> keys;

    private final byte[] body;

    /** message consume times */
    private final int deliveryAttempt;

    /** trust message born timestamp, we don't care store timestamp */
    private final long eventTime;

    private final long ingestionTime;

    private final Map<String, String> properties;

    public MessageViewExt(MessageExt messageExt) {
        this.messageId = messageExt.getMsgId();
        this.topic = messageExt.getTopic();
        this.brokerName = messageExt.getBrokerName();
        this.queueId = messageExt.getQueueId();
        this.queueOffset = messageExt.getQueueOffset();
        this.storeSize = messageExt.getStoreSize();
        this.tag = messageExt.getTags();
        this.keys =
                messageExt.getKeys() != null
                        ? Arrays.asList(messageExt.getKeys().split(KEY_SEPARATE))
                        : new ArrayList<>();
        this.body = messageExt.getBody();
        this.deliveryAttempt = messageExt.getReconsumeTimes();
        this.eventTime = messageExt.getBornTimestamp();
        this.ingestionTime = System.currentTimeMillis();
        this.properties = messageExt.getProperties();
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getBrokerName() {
        return brokerName;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public long getQueueOffset() {
        return queueOffset;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public Collection<String> getKeys() {
        return keys;
    }

    @Override
    public int getStoreSize() {
        return storeSize;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public int getDeliveryAttempt() {
        return deliveryAttempt;
    }

    @Override
    public long getEventTime() {
        return eventTime;
    }

    @Override
    public long getIngestionTime() {
        return ingestionTime;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "MessageViewExt{"
                + "messageId='"
                + messageId
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", brokerName='"
                + brokerName
                + '\''
                + ", queueId="
                + queueId
                + ", queueOffset="
                + queueOffset
                + ", storeSize="
                + storeSize
                + ", tag='"
                + tag
                + '\''
                + ", keys="
                + keys
                + ", deliveryAttempt="
                + deliveryAttempt
                + ", eventTime="
                + eventTime
                + ", ingestionTime="
                + ingestionTime
                + '}';
    }
}
