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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.connectors.rocketmq.common.util.TestUtils;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.fail;

/**
 * The legacy sink parks the first asynchronous send failure and rethrows it later. These tests pin
 * down <em>where</em> it is rethrown: a checkpoint must not complete over records whose send
 * already failed, otherwise an idle or finished stream commits the loss while every health signal
 * stays green.
 */
public class RocketMQSinkAsyncFailureTest {

    private static final FunctionSnapshotContext SNAPSHOT =
            new FunctionSnapshotContext() {
                @Override
                public long getCheckpointId() {
                    return 1L;
                }

                @Override
                public long getCheckpointTimestamp() {
                    return 1L;
                }
            };

    @Test
    public void snapshotStateSurfacesParkedAsyncSendFailureTest() throws Exception {
        RocketMQSink sink = new RocketMQSink(new Properties());
        DefaultMQProducer producer = Mockito.mock(DefaultMQProducer.class);
        Mockito.doAnswer(
                        invocation -> {
                            SendCallback callback = invocation.getArgument(1);
                            callback.onException(new IOException("CODE: 13 message rejected"));
                            return null;
                        })
                .when(producer)
                .send(Mockito.any(Message.class), Mockito.any(SendCallback.class));
        TestUtils.setFieldValue(sink, "producer", producer);
        TestUtils.setFieldValue(sink, "async", true);
        TestUtils.setFieldValue(
                sink, "sinkInTps", Mockito.mock(org.apache.flink.metrics.Meter.class));

        // The burst of records is sent before any callback fires, so invoke() itself
        // returns normally and only parks the failure.
        sink.invoke(new Message("topic", "body".getBytes()), null);

        try {
            sink.snapshotState(SNAPSHOT);
            fail("snapshotState must fail the checkpoint when an async send already failed");
        } catch (IOException expected) {
            // the parked failure must surface here, not be committed over
        }
    }

    @Test
    public void snapshotStatePassesWhenNoSendFailedTest() throws Exception {
        RocketMQSink sink = new RocketMQSink(new Properties());
        sink.snapshotState(SNAPSHOT);
    }
}
