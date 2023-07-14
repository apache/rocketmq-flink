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

package org.apache.flink.connector.rocketmq.source.metrics;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;

import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class RocketMQSourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceReaderMetrics.class);

    public static final String ROCKETMQ_SOURCE_READER_METRIC_GROUP = "RocketmqSourceReader";
    public static final String TOPIC_GROUP = "topic";
    public static final String QUEUE_GROUP = "queue";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String COMMITTED_OFFSET_METRIC_GAUGE = "committedOffset";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";
    public static final String ROCKETMQ_CONSUMER_METRIC_GROUP = "RocketMQConsumer";

    public static final String CONSUMER_FETCH_MANAGER_GROUP = "consumer-fetch-manager-metrics";
    public static final String BYTES_CONSUMED_TOTAL = "bytes-consumed-total";
    public static final String RECORDS_LAG = "records-lag";

    public RocketMQSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {}

    public void registerNewMessageQueue(MessageQueue messageQueue) {}
}
