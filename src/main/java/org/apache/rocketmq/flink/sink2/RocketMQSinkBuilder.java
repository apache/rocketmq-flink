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

package org.apache.rocketmq.flink.sink2;

import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQMessageSerializationSchema;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RocketMQSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSinkBuilder.class);

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;

    private String producerGroup = "rocketmq-sink";

    private final Properties rocketmqProducerConfig;

    private RocketMQMessageSerializationSchema<IN> recordSerializer;

    public RocketMQSinkBuilder() {
        rocketmqProducerConfig = new Properties();
    }

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Sets the configuration which used to instantiate all used
     *
     * @param props
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setRocketmqProducerConfig(Properties props) {
        checkNotNull(props);
        rocketmqProducerConfig.putAll(props);
        return this;
    }

    /**
     * Sets the RocketMQ bootstrap servers.
     *
     * @param nameServerAddr a comma separated list of valid URIs to reach the rocketmq broker
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setBootstrapServers(String nameServerAddr) {
        return setProperty(RocketMQConfig.NAME_SERVER_ADDR, nameServerAddr);
    }

    /**
     * Sets the RocketMQ producer group.
     *
     * @param producerGroup
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setProducerGroup(String producerGroup) {
        this.producerGroup = checkNotNull(producerGroup, "producerGroup");
        return this;
    }

    public RocketMQSinkBuilder<IN> setProperty(String key, String value) {
        checkNotNull(key);
        rocketmqProducerConfig.setProperty(key, value);
        return this;
    }

    /**
     * Sets the {@link RocketMQMessageSerializationSchema} that transforms incoming records to
     * {@link org.apache.rocketmq.common.message.Message}s.
     *
     * @param recordSerializer
     * @return {@link RocketMQSinkBuilder}
     */
    public RocketMQSinkBuilder<IN> setRecordSerializer(
            RocketMQMessageSerializationSchema<IN> recordSerializer) {
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        ClosureCleaner.clean(
                this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    private void sanityCheck() {
        // TODO: 2022/11/3 add sanity check
    }

    public RocketMQSink build() {
        sanityCheck();
        return new RocketMQSink<>(
                deliveryGuarantee, rocketmqProducerConfig, producerGroup, recordSerializer);
    }
}
