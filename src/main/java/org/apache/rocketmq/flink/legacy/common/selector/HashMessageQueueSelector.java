package org.apache.rocketmq.flink.legacy.common.selector;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class HashMessageQueueSelector implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = arg.hashCode() % mqs.size();
        if (value < 0) {
            value = Math.abs(value);
        }
        return mqs.get(value);
    }
}
