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

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RocketMQGrpcSourceBuilder}. */
class RocketMQGrpcSourceBuilderTest {

    @Test
    void buildSucceedsWithAllRequiredOptionsTest() {
        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints("127.0.0.1:8080")
                        .setConsumerGroup("group")
                        .setMode(ConsumerMode.LITE)
                        .setMainTopic("topic")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        assertThat(source).isNotNull();
    }

    @Test
    void buildFailsWithoutEndpointsTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setConsumerGroup("group")
                                        .setMode(ConsumerMode.LITE)
                                        .setMainTopic("topic")
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("endpoints");
    }

    @Test
    void buildFailsWithoutConsumerGroupTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setMode(ConsumerMode.LITE)
                                        .setMainTopic("topic")
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("consumer group");
    }

    @Test
    void buildFailsWithoutMainTopicTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setConsumerGroup("group")
                                        .setMode(ConsumerMode.LITE)
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("main topic");
    }

    @Test
    void buildFailsWithoutDeserializerTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setConsumerGroup("group")
                                        .setMode(ConsumerMode.LITE)
                                        .setMainTopic("topic")
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("deserializer");
    }

    @Test
    void buildSucceedsWithSimpleModeTest() {
        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints("127.0.0.1:8080")
                        .setConsumerGroup("group")
                        .setMode(ConsumerMode.SIMPLE)
                        .setTopic("normal-topic")
                        .setFilterExpression("tagA||tagB")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        assertThat(source).isNotNull();
    }

    @Test
    void buildFailsWithSimpleModeWithoutTopicTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setConsumerGroup("group")
                                        .setMode(ConsumerMode.SIMPLE)
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void buildFailsWithLiteModeWhenOnlySimpleTopicIsSetTest() {
        assertThatThrownBy(
                        () ->
                                RocketMQGrpcSource.<String>builder()
                                        .setEndpoints("127.0.0.1:8080")
                                        .setConsumerGroup("group")
                                        .setMode(ConsumerMode.LITE)
                                        .setTopic("normal-topic")
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("main topic");
    }

    @Test
    void buildDefaultsToSimpleModeTest() {
        // Without setMode() the builder must run in SIMPLE mode: a LITE-mode build would reject
        // the missing main topic.
        final RocketMQGrpcSource<String> source =
                RocketMQGrpcSource.<String>builder()
                        .setEndpoints("127.0.0.1:8080")
                        .setConsumerGroup("group")
                        .setTopic("normal-topic")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        assertThat(source).isNotNull();
        assertThat(RocketMQGrpcSourceOptions.MODE.defaultValue()).isEqualTo(ConsumerMode.SIMPLE);
    }
}
