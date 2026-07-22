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

package org.apache.flink.connector.rocketmq.sink.writer.context;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.configuration.Configuration;

@PublicEvolving
public class RocketMQSinkContextImpl implements RocketMQSinkContext {

    private final int numberOfParallelSubtasks;
    private final int parallelInstanceId;
    private final ProcessingTimeService processingTimeService;
    private final MailboxExecutor mailboxExecutor;
    private final boolean enableSchemaEvolution;

    public RocketMQSinkContextImpl(InitContext initContext, Configuration configuration) {
        this.parallelInstanceId = initContext.getSubtaskId();
        this.numberOfParallelSubtasks = initContext.getNumberOfParallelSubtasks();
        this.processingTimeService = initContext.getProcessingTimeService();
        this.mailboxExecutor = initContext.getMailboxExecutor();
        this.enableSchemaEvolution = false;
    }

    @Override
    public int getParallelInstanceId() {
        return parallelInstanceId;
    }

    @Override
    public int getNumberOfParallelInstances() {
        return numberOfParallelSubtasks;
    }

    @Override
    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

    @Override
    public long processTime() {
        return processingTimeService.getCurrentProcessingTime();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }
}
