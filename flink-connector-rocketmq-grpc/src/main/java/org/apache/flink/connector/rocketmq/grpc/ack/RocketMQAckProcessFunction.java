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

package org.apache.flink.connector.rocketmq.grpc.ack;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.time.Duration;
import java.util.Objects;

/**
 * An abstract {@link ProcessFunction} that gives subclasses a ready-to-use {@link
 * RocketMQLiteAckClient} so they can acknowledge or re-schedule RocketMQ Pop messages directly from
 * their own business logic via {@link #ack} and {@link #changeInvisibleDuration}.
 *
 * <p>The client is shared per TaskManager through {@link RocketMQLiteAckClient#acquire}: it is
 * acquired in {@link #open(OpenContext)} and released in {@link #close()}. The RocketMQ client
 * options (endpoints, namespace, credentials, TLS, timeout) are provided through the {@link
 * Configuration} passed to the constructor and never travel in the data stream.
 *
 * @param <IN> the input record type, typically {@code AckableMessage<T>}.
 * @param <OUT> the output record type.
 */
@PublicEvolving
public abstract class RocketMQAckProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private final Configuration configuration;

    private transient RocketMQLiteAckClient ackClient;
    private transient boolean acquired;
    private transient Counter numAcksSucceeded;
    private transient Counter numAcksFailed;
    private transient Counter numInvisibleDurationChangesSucceeded;
    private transient Counter numInvisibleDurationChangesFailed;

    protected RocketMQAckProcessFunction(Configuration configuration) {
        this.configuration =
                new Configuration(
                        Objects.requireNonNull(configuration, "configuration should not be null"));
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.ackClient = RocketMQLiteAckClient.acquire(configuration);
        this.acquired = true;
        this.numAcksSucceeded = getRuntimeContext().getMetricGroup().counter("numAcksSucceeded");
        this.numAcksFailed = getRuntimeContext().getMetricGroup().counter("numAcksFailed");
        this.numInvisibleDurationChangesSucceeded =
                getRuntimeContext()
                        .getMetricGroup()
                        .counter("numInvisibleDurationChangesSucceeded");
        this.numInvisibleDurationChangesFailed =
                getRuntimeContext().getMetricGroup().counter("numInvisibleDurationChangesFailed");
    }

    @Override
    public void close() throws Exception {
        // Only release when open() actually acquired the client; Flink calls close() even after a
        // failed open(), and a stray release would decrement another operator's reference.
        if (acquired) {
            RocketMQLiteAckClient.release(configuration);
            acquired = false;
        }
        this.ackClient = null;
        super.close();
    }

    /** Acknowledge the message described by the handle so it is not re-delivered. */
    protected void ack(RocketMQReceiptHandle handle) {
        try {
            ackClient.ack(handle);
            numAcksSucceeded.inc();
        } catch (RuntimeException e) {
            numAcksFailed.inc();
            throw e;
        }
    }

    /**
     * Change the invisible duration of the message described by the handle, deferring its next
     * re-delivery by {@code invisibleDuration}.
     */
    protected void changeInvisibleDuration(
            RocketMQReceiptHandle handle, Duration invisibleDuration) {
        try {
            ackClient.changeInvisibleDuration(handle, invisibleDuration);
            numInvisibleDurationChangesSucceeded.inc();
        } catch (RuntimeException e) {
            numInvisibleDurationChangesFailed.inc();
            throw e;
        }
    }
}
