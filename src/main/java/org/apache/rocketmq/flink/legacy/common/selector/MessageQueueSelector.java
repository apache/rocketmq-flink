package org.apache.rocketmq.flink.legacy.common.selector;

import java.io.Serializable;

public interface MessageQueueSelector
        extends org.apache.rocketmq.client.producer.MessageQueueSelector, Serializable {}
