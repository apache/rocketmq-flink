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

package org.apache.flink.connector.rocketmq.grpc.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RocketMQGrpcSinkBuilder}. */
class RocketMQGrpcSinkBuilderTest {

    @Test
    void buildSucceedsWithValueOnlySerializerTest() {
        final RocketMQGrpcSink<String> sink =
                RocketMQGrpcSink.<String>builder()
                        .setEndpoints("127.0.0.1:8080")
                        .setTopic("topic")
                        .setLiteTopic("lite-topic")
                        .setValueOnlySerializer(new SimpleStringSchema())
                        .build();
        assertThat(sink).isNotNull();
    }

    @Test
    void buildFailsWithoutEndpointsTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSink.<String>builder()
                                        .setTopic("topic")
                                        .setLiteTopic("lite-topic")
                                        .setValueOnlySerializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("endpoints");
    }

    @Test
    void buildFailsWithoutSerializerTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSink.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setTopic("topic")
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void valueOnlySerializerRequiresTopicTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSink.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setValueOnlySerializer(new SimpleStringSchema()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void valueOnlySerializerRequiresLiteTopicTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSink.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setTopic("topic")
                                        .setValueOnlySerializer(new SimpleStringSchema()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("lite topic");
    }
}
