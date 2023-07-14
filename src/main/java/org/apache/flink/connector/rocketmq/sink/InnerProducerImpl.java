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

package org.apache.flink.connector.rocketmq.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.sink.committer.SendCommittable;
import org.apache.flink.util.StringUtils;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class InnerProducerImpl implements InnerProducer {

    private static final Logger LOG = LoggerFactory.getLogger(InnerProducerImpl.class);

    private final Configuration configuration;
    private final TransactionMQProducer producer;
    private MQClientInstance mqClientInstance;

    private final String endPoints;
    private final String groupId;

    public InnerProducerImpl(Configuration configuration) {
        this.configuration = configuration;
        this.groupId = configuration.getString(RocketMQSinkOptions.PRODUCER_GROUP);
        this.endPoints = configuration.getString(RocketMQSinkOptions.ENDPOINTS);

        String accessKey = configuration.getString(RocketMQSinkOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(RocketMQSinkOptions.OPTIONAL_SECRET_KEY);

        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            AclClientRPCHook aclClientRpcHook =
                    new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
            producer = new TransactionMQProducer(groupId, aclClientRpcHook);
        } else {
            producer = new TransactionMQProducer(groupId);
        }

        producer.setNamesrvAddr(endPoints);
        producer.setVipChannelEnabled(false);
        producer.setInstanceName(
                String.join(
                        "#",
                        ManagementFactory.getRuntimeMXBean().getName(),
                        groupId,
                        UUID.randomUUID().toString()));

        int corePoolSize = configuration.getInteger(RocketMQSinkOptions.EXECUTOR_NUM);
        producer.setExecutorService(
                new ThreadPoolExecutor(
                        corePoolSize,
                        corePoolSize,
                        100,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(2000),
                        r -> {
                            Thread thread = new Thread(r);
                            thread.setName(groupId);
                            return thread;
                        }));

        // always response unknown result
        producer.setTransactionListener(
                new TransactionListener() {
                    @Override
                    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                        // no need execute local transaction here
                        // We will directly call the commit or rollback operation
                        return LocalTransactionState.UNKNOW;
                    }

                    @Override
                    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                        long transactionTimeout =
                                configuration.get(RocketMQSinkOptions.TRANSACTION_TIMEOUT);
                        if (System.currentTimeMillis() - msg.getBornTimestamp()
                                > transactionTimeout) {
                            LOG.info(
                                    "Exceeded the transaction maximum time, return rollback. topic={}, msgId={}",
                                    msg.getTopic(),
                                    msg.getTransactionId());
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        } else {
                            LOG.info(
                                    "Not exceeded the transaction maximum time, return unknown. topic={}, msgId={}",
                                    msg.getTopic(),
                                    msg.getTransactionId());
                            return LocalTransactionState.UNKNOW;
                        }
                    }
                });
    }

    @Override
    public void start() {
        try {
            producer.start();
            // noinspection deprecation
            mqClientInstance = producer.getDefaultMQProducerImpl().getMqClientFactory();
            LOG.info(
                    "RocketMQ producer in flink sink writer init success, endpoint={}, groupId={}, clientId={}",
                    endPoints,
                    groupId,
                    producer.getInstanceName());
        } catch (MQClientException e) {
            LOG.error("RocketMQ producer in flink sink writer init failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getProducerGroup() {
        return groupId;
    }

    @Override
    public CompletableFuture<SendResult> send(Message message) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        SendResult sendResult = producer.send(message);
                        LOG.info(
                                "Send message successfully, topic={}, messageId={}",
                                message.getTopic(),
                                sendResult.getMsgId());
                        return sendResult;
                    } catch (Exception e) {
                        LOG.error("Failed to send message, topic={}", message.getTopic(), e);
                        throw new RuntimeException(e);
                    }
                },
                MoreExecutors.directExecutor());
    }

    @Override
    public CompletableFuture<SendResult> sendMessageInTransaction(Message message) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        message.setTopic(
                                NamespaceUtil.wrapNamespace(
                                        producer.getNamespace(), message.getTopic()));

                        // Ignore DelayTimeLevel parameter
                        if (message.getDelayTimeLevel() != 0) {
                            MessageAccessor.clearProperty(
                                    message, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                        }

                        // In general, message id and transaction id should be the same
                        long transactionTimeout =
                                configuration.get(RocketMQSinkOptions.TRANSACTION_TIMEOUT);
                        message.putUserProperty(
                                MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                                String.valueOf(transactionTimeout));
                        MessageAccessor.putProperty(
                                message,
                                MessageConst.PROPERTY_TRANSACTION_PREPARED,
                                Boolean.TRUE.toString().toLowerCase());
                        MessageAccessor.putProperty(
                                message, MessageConst.PROPERTY_PRODUCER_GROUP, this.groupId);

                        SendResult sendResult = producer.send(message);
                        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
                            LOG.info(
                                    "Send transaction message successfully, topic={}, transId={}",
                                    message.getTopic(),
                                    sendResult.getTransactionId());
                        } else {
                            LOG.warn(
                                    "Failed to send message, topic={}, message={}",
                                    message.getTopic(),
                                    message);
                        }
                        return sendResult;
                    } catch (Exception e) {
                        LOG.error("Failed to send message, topic={}", message.getTopic(), e);
                        throw new RuntimeException(e);
                    }
                },
                MoreExecutors.directExecutor());
    }

    public void endTransaction(
            final SendCommittable sendCommittable, final TransactionResult transactionResult) {

        try {
            final String brokerName =
                    this.mqClientInstance.getBrokerNameFromMessageQueue(
                            producer.queueWithNamespace(sendCommittable.getMessageQueue()));
            final String brokerAddress =
                    this.mqClientInstance.findBrokerAddressInPublish(brokerName);

            EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
            requestHeader.setTransactionId(sendCommittable.getTransactionId());
            requestHeader.setCommitLogOffset(sendCommittable.getMessageOffset());
            requestHeader.setBname(brokerName);
            requestHeader.setProducerGroup(this.groupId);
            requestHeader.setTranStateTableOffset(sendCommittable.getQueueOffset());
            requestHeader.setFromTransactionCheck(true);
            requestHeader.setMsgId(sendCommittable.getMsgId());

            switch (transactionResult) {
                case COMMIT:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                    break;
                case ROLLBACK:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                    break;
                case UNKNOWN:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                    break;
                default:
                    break;
            }

            if (sendCommittable.getMessageOffset() != -1L) {
                this.endTransaction(
                        brokerAddress, requestHeader, "", this.producer.getSendMsgTimeout());
            } else {
                LOG.error(
                        "Convert message physical offset error, msgId={}",
                        sendCommittable.getMsgId());
            }
        } catch (Exception e) {
            LOG.error("Try end transaction error", e);
        }
    }

    public void endTransaction(
            final String address,
            final EndTransactionRequestHeader requestHeader,
            final String remark,
            final long timeoutMillis)
            throws RemotingException, InterruptedException {
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        request.setRemark(remark);
        this.mqClientInstance
                .getMQClientAPIImpl()
                .getRemotingClient()
                .invokeSync(address, request, timeoutMillis);
    }

    @Override
    public CompletableFuture<Void> commit(SendCommittable sendCommittable) {
        return CompletableFuture.runAsync(
                () -> endTransaction(sendCommittable, TransactionResult.COMMIT),
                producer.getExecutorService());
    }

    @Override
    public CompletableFuture<Void> rollback(SendCommittable sendCommittable) {
        return CompletableFuture.runAsync(
                () -> endTransaction(sendCommittable, TransactionResult.ROLLBACK),
                producer.getExecutorService());
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            String clientId = producer.getInstanceName();
            producer.shutdown();
            mqClientInstance = null;
            LOG.info("RocketMQ producer has shutdown, groupId={}, clientId={}", groupId, clientId);
        }
    }
}
