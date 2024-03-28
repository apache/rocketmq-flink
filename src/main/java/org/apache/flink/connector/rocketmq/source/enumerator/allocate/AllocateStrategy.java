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

package org.apache.flink.connector.rocketmq.source.enumerator.allocate;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/** This interface defines a strategy for allocating RocketMQ source splits to Flink tasks. */
@PublicEvolving
public interface AllocateStrategy {

    /**
     * Allocate strategy name
     *
     * @return Current strategy name
     */
    String getStrategyName();

    /**
     * Allocates RocketMQ source splits to Flink tasks based on the selected allocation strategy.
     *
     * @param mqAll a collection of all available RocketMQ source splits
     * @param parallelism the desired parallelism for the Flink tasks
     * @return a map of task indices to sets of corresponding RocketMQ source splits
     */
    Map<Integer, Set<RocketMQSourceSplit>> allocate(
            final Collection<RocketMQSourceSplit> mqAll, final int parallelism);
}
