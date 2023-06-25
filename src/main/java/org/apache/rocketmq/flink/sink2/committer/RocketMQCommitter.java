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

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.flink.sink2.RocketMQSink;

import org.apache.flink.api.connector.sink2.Committer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;

/**
 * Committer implementation for {@link RocketMQSink}.
 *
 * <p>The committer is responsible to finalize the Pulsar transactions by committing them.
 */
public class RocketMQCommitter implements Committer<RocketMQCommittable>, TransactionListener {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQCommitter.class);

    private final ArrayDeque<String> needToCommitMsgTransactionIds;

    public RocketMQCommitter() {
        this.needToCommitMsgTransactionIds = new ArrayDeque<>();
    }

    @Override
    public void commit(Collection<CommitRequest<RocketMQCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<RocketMQCommittable> request : requests) {
            final RocketMQCommittable committable = request.getCommittable();
            final String transactionalId = committable.getMsgId();
            LOG.debug("Committing RocketMQ transaction {}", transactionalId);
            needToCommitMsgTransactionIds.addLast(transactionalId);
        }
    }

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return needToCommitMsgTransactionIds.remove(msg.getTransactionId())
                ? LocalTransactionState.COMMIT_MESSAGE
                : LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        return needToCommitMsgTransactionIds.remove(msg.getTransactionId())
                ? LocalTransactionState.COMMIT_MESSAGE
                : LocalTransactionState.ROLLBACK_MESSAGE;
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
