/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.grpc.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.connector.rocketmq.grpc.source.deserialization.RocketMQGrpcDeserializationSchema;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A fluent builder to construct a {@link RocketMQGrpcSource}. */
@PublicEvolving
public class RocketMQGrpcSourceBuilder<OUT> {

    private final Configuration configuration;
    private String mainTopic;
    private Boundedness boundedness;
    private RocketMQGrpcDeserializationSchema<OUT> deserializationSchema;

    public RocketMQGrpcSourceBuilder() {
        this.configuration = new Configuration();
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /** Set the gRPC access point (proxy) endpoints. */
    public RocketMQGrpcSourceBuilder<OUT> setEndpoints(String endpoints) {
        return setConfig(RocketMQGrpcOptions.ENDPOINTS, endpoints);
    }

    /** Set the consumer group of the LiteSimpleConsumer. */
    public RocketMQGrpcSourceBuilder<OUT> setConsumerGroup(String consumerGroup) {
        return setConfig(RocketMQGrpcSourceOptions.CONSUMER_GROUP, consumerGroup);
    }

    /** Set the main lite topic bound by the LiteSimpleConsumer. Every subtask binds this topic. */
    public RocketMQGrpcSourceBuilder<OUT> setMainTopic(String mainTopic) {
        checkArgument(
                mainTopic != null && !mainTopic.trim().isEmpty(),
                "main topic must not be null or blank");
        this.mainTopic = mainTopic;
        return this;
    }

    /**
     * Set the fetch concurrency of each subtask, i.e. the number of concurrent fetch requests it
     * issues. The requests are served by worker threads sharing a single thread-safe {@code
     * SimpleConsumer}. Defaults to {@code 1}.
     */
    public RocketMQGrpcSourceBuilder<OUT> setFetchConcurrency(int fetchConcurrency) {
        return setConfig(RocketMQGrpcSourceOptions.FETCH_CONCURRENCY, fetchConcurrency);
    }

    /**
     * Set the fully qualified class name of an {@link InvisibleDurationRenewalPolicy}. When
     * configured, the source consults the policy shortly before a still-buffered message would
     * become visible again, and the policy decides whether to extend its invisible duration.
     */
    public RocketMQGrpcSourceBuilder<OUT> setRenewalPolicyClass(String renewalPolicyClass) {
        return setConfig(RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS, renewalPolicyClass);
    }

    /**
     * Set how long before a buffered message becomes visible again the renewal policy is consulted.
     * Defaults to 5 seconds, i.e. with a 60s invisible duration the policy runs 55s after the
     * message was received.
     */
    public RocketMQGrpcSourceBuilder<OUT> setRenewalAheadTime(Duration aheadTime) {
        return setConfig(RocketMQGrpcSourceOptions.RENEWAL_AHEAD_TIME, aheadTime);
    }

    /** Set the boundedness of this source. Defaults to {@link Boundedness#CONTINUOUS_UNBOUNDED}. */
    public RocketMQGrpcSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
        this.boundedness = checkNotNull(boundedness);
        return this;
    }

    /** Set the {@link RocketMQGrpcDeserializationSchema}. */
    public RocketMQGrpcSourceBuilder<OUT> setDeserializer(
            RocketMQGrpcDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = checkNotNull(deserializationSchema);
        return this;
    }

    /** Set a value-only deserializer that decodes the message body using a Flink schema. */
    public RocketMQGrpcSourceBuilder<OUT> setValueOnlyDeserializer(
            DeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema =
                RocketMQGrpcDeserializationSchema.flinkSchema(deserializationSchema);
        return this;
    }

    /** Set an arbitrary configuration option. */
    public <T> RocketMQGrpcSourceBuilder<OUT> setConfig(ConfigOption<T> key, T value) {
        configuration.set(key, value);
        return this;
    }

    /** Add arbitrary configuration options. */
    public RocketMQGrpcSourceBuilder<OUT> setConfig(Configuration config) {
        configuration.addAll(config);
        return this;
    }

    /** Build the {@link RocketMQGrpcSource}. */
    public RocketMQGrpcSource<OUT> build() {
        checkNotNull(
                configuration.get(RocketMQGrpcOptions.ENDPOINTS), "endpoints must be configured");
        checkNotNull(
                configuration.get(RocketMQGrpcSourceOptions.CONSUMER_GROUP),
                "consumer group must be configured");
        checkArgument(
                mainTopic != null && !mainTopic.trim().isEmpty(),
                "the main topic must be configured");
        checkArgument(
                configuration.get(RocketMQGrpcSourceOptions.FETCH_CONCURRENCY) >= 1,
                "fetch concurrency must be at least 1");
        checkNotNull(deserializationSchema, "deserializer must be configured");
        configuration.set(RocketMQGrpcSourceOptions.MAIN_TOPIC, mainTopic);
        return new RocketMQGrpcSource<>(configuration, boundedness, deserializationSchema);
    }
}
