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

package org.apache.flink.connector.rocketmq.example;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class SimpleAdmin implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SimpleAdmin.class);

    private final DefaultMQAdminExt adminExt;

    public SimpleAdmin() throws MQClientException {
        this.adminExt = new DefaultMQAdminExt(ConnectorConfig.getAclRpcHook(), 6 * 1000L);
        this.adminExt.setNamesrvAddr(ConnectorConfig.ENDPOINTS);
        this.adminExt.setVipChannelEnabled(false);
        this.adminExt.setAdminExtGroup(ConnectorConfig.ADMIN_TOOL_GROUP);
        this.adminExt.setInstanceName(
                adminExt.getAdminExtGroup().concat("-").concat(UUID.randomUUID().toString()));
        this.adminExt.start();
        log.info(
                "Initialize rocketmq simple admin tools success, name={}",
                adminExt.getInstanceName());
    }

    @Override
    public void close() {
        this.adminExt.shutdown();
    }

    private Set<String> getBrokerAddress()
            throws RemotingException, InterruptedException, MQClientException {
        return adminExt.examineTopicRouteInfo(ConnectorConfig.CLUSTER_NAME).getBrokerDatas()
                .stream()
                .map(BrokerData::selectBrokerAddr)
                .collect(Collectors.toSet());
    }

    private void createTestTopic(Set<String> brokerAddressSet)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {

        for (String brokerAddress : brokerAddressSet) {
            adminExt.createAndUpdateTopicConfig(
                    brokerAddress,
                    new TopicConfig(
                            ConnectorConfig.SOURCE_TOPIC_1,
                            2,
                            2,
                            PermName.PERM_READ | PermName.PERM_WRITE));

            adminExt.createAndUpdateTopicConfig(
                    brokerAddress,
                    new TopicConfig(
                            ConnectorConfig.SOURCE_TOPIC_2,
                            2,
                            2,
                            PermName.PERM_READ | PermName.PERM_WRITE));

            log.info("Create topic success, brokerAddress={}", brokerAddress);
        }
    }

    private void deleteSubscriptionGroup(Set<String> brokerAddressSet)
            throws MQBrokerException, RemotingException, InterruptedException, MQClientException {

        for (String brokerAddress : brokerAddressSet) {
            adminExt.deleteSubscriptionGroup(brokerAddress, ConnectorConfig.CONSUMER_GROUP, true);
            log.info("Delete consumer group success, brokerAddress={}", brokerAddress);
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleAdmin simpleAdmin = new SimpleAdmin();
        Set<String> brokerAddressSet = simpleAdmin.getBrokerAddress();
        simpleAdmin.deleteSubscriptionGroup(brokerAddressSet);
        simpleAdmin.createTestTopic(brokerAddressSet);
        simpleAdmin.close();
    }
}
