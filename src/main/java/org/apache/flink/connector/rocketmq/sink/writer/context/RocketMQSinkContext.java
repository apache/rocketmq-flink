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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;

/**
 * This context provides information on the rocketmq record target location. An implementation that
 * would contain all the required context.
 */
@PublicEvolving
public interface RocketMQSinkContext {

    /**
     * Get the number of the subtask that RocketMQSink is running on. The numbering starts from 0
     * and goes up to parallelism-1. (parallelism as returned by {@link
     * #getNumberOfParallelInstances()}
     *
     * @return number of subtask
     */
    int getParallelInstanceId();

    /** @return number of parallel RocketMQSink tasks. */
    int getNumberOfParallelInstances();

    /**
     * RocketMQ can check the schema and upgrade the schema automatically. If you enable this
     * option, we wouldn't serialize the record into bytes, we send and serialize it in the client.
     */
    @Experimental
    boolean isEnableSchemaEvolution();

    /** Returns the current process time in flink. */
    long processTime();

    MailboxExecutor getMailboxExecutor();
}
