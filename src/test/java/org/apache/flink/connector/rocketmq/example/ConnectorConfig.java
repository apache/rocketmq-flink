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

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

public class ConnectorConfig {

    // for rocketmq cluster
    public static final String ENDPOINTS = "127.0.0.1:9876";
    public static final String CLUSTER_NAME = "flink-cluster";

    // for admin
    public static final String ADMIN_TOOL_GROUP = "rocketmq-tools";

    // for producer and consumer
    public static final String PRODUCER_GROUP = "PID-flink";
    public static final String CONSUMER_GROUP = "GID-flink";
    public static final String SOURCE_TOPIC_1 = "flink-source-1";
    public static final String SOURCE_TOPIC_2 = "flink-source-2";
    public static final String SINK_TOPIC_1 = "flink-sink-1";
    public static final String SINK_TOPIC_2 = "flink-sink-2";

    // for message
    public static final String TAGS = "*";
    public static final String KEY_PREFIX = "key_";
    public static final int MESSAGE_NUM = 100;

    // for authentication
    public static final String ACCESS_KEY = "accessKey";
    public static final String SECRET_KEY = "secretKey";

    public static RPCHook getAclRpcHook() {
        return new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY));
    }
}
