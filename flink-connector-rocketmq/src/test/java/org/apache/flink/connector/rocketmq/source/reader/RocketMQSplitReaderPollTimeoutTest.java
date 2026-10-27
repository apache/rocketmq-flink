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

package org.apache.flink.connector.rocketmq.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.metrics.RocketMQSourceReaderMetrics;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQDeserializationSchema;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@code rocketmq.client.poll.timeout} reaches the consumer poll and that its default no
 * longer spins the fetch loop of an idle job a hundred times a second. The bound matters because
 * {@link RocketMQSplitReader#wakeUp()} only sets a flag and the consumer deliberately has no
 * interruptible poll, so the timeout is what limits how long a fetch can outlive a wakeup.
 */
class RocketMQSplitReaderPollTimeoutTest {

    @Test
    void idleFetchBlocksForTheDefaultTimeoutTest() {
        assertThat(pollTimeoutOf(new Configuration())).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void idleFetchBlocksForTheConfiguredTimeoutTest() {
        Configuration configuration = new Configuration();
        configuration.set(RocketMQOptions.POLL_TIMEOUT, 250L);

        assertThat(pollTimeoutOf(configuration)).isEqualTo(Duration.ofMillis(250));
    }

    @SuppressWarnings("unchecked")
    private static Duration pollTimeoutOf(Configuration configuration) {
        InnerConsumer consumer = Mockito.mock(InnerConsumer.class);
        Mockito.when(consumer.poll(Mockito.any())).thenReturn(Collections.emptyList());

        RocketMQSplitReader<String> reader =
                new RocketMQSplitReader<>(
                        configuration,
                        Mockito.mock(SourceReaderContext.class),
                        Mockito.mock(RocketMQDeserializationSchema.class),
                        Mockito.mock(RocketMQSourceReaderMetrics.class),
                        consumer);
        try {
            reader.fetch();
        } catch (IOException e) {
            throw new AssertionError("fetching without records must not fail", e);
        }

        ArgumentCaptor<Duration> timeout = ArgumentCaptor.forClass(Duration.class);
        Mockito.verify(consumer).poll(timeout.capture());
        return timeout.getValue();
    }
}
