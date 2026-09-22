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

/** The consumption mode of the RocketMQ gRPC source. */
@PublicEvolving
public enum ConsumerMode {

    /**
     * Bind a main lite topic via a {@code LiteSimpleConsumer}; the broker delivers messages of all
     * sub topics over one receive stream. Messages are acknowledged by a downstream operator using
     * the receipt handle carried by each emitted record.
     */
    LITE,

    /**
     * Subscribe to a normal topic via a {@code SimpleConsumer} with an optional filter expression.
     * Messages are acknowledged by the source itself once the enclosing checkpoint completes
     * (at-least-once); checkpointing must be enabled and the invisible duration must exceed the
     * checkpoint interval plus timeout.
     */
    SIMPLE
}
