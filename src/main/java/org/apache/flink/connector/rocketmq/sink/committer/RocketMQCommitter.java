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

package org.apache.flink.connector.rocketmq.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.sink.InnerProducer;
import org.apache.flink.connector.rocketmq.sink.InnerProducerImpl;
import org.apache.flink.connector.rocketmq.sink.RocketMQSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link RocketMQSink}
 *
 * <p>The committer is responsible to finalize the RocketMQ transactions by committing them.
 */
public class RocketMQCommitter implements Committer<SendCommittable>, Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQCommitter.class);
    private InnerProducer producer;
    private final Configuration configuration;

    public RocketMQCommitter(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void commit(Collection<CommitRequest<SendCommittable>> requests) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            for (CommitRequest<SendCommittable> request : requests) {
                final SendCommittable committable = request.getCommittable();
                LOG.info("Commit transaction message, send committable={}", committable);
                try {
                    CompletableFuture<Void> future =
                            this.getTransactionProducer()
                                    .commit(committable)
                                    .thenAccept(unused -> request.signalAlreadyCommitted())
                                    .exceptionally(
                                            throwable -> {
                                                LOG.error(
                                                        "Commit message error, committable={}",
                                                        committable);
                                                request.signalFailedWithKnownReason(throwable);
                                                return null;
                                            });
                    futures.add(future);
                } catch (Throwable e) {
                    LOG.error("Commit message error, committable={}", committable, e);
                    request.signalFailedWithKnownReason(e);
                }
            }
            CompletableFuture<Void> allFuture =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allFuture.get();
        } catch (Exception e) {
            LOG.error("Commit message error", e);
        }
    }

    /** Lazy initialize this backend transaction client. */
    private InnerProducer getTransactionProducer() {
        if (producer == null) {
            this.producer = new InnerProducerImpl(configuration);
            this.producer.start();
            checkNotNull(producer, "You haven't enable rocketmq transaction client.");
        }
        return producer;
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public RocketMQCommitter clone() {
        try {
            // TODO: copy mutable state here, so the clone can't change the internals of the
            // original
            return (RocketMQCommitter) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
