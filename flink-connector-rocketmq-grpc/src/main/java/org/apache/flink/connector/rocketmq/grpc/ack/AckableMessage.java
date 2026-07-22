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

import java.util.Objects;

/**
 * The element type produced by the RocketMQ gRPC source when running in downstream-acknowledgement
 * mode. It pairs the deserialized record {@code value} with the credential-free {@link
 * RocketMQReceiptHandle} needed to acknowledge (or re-schedule) the originating message from any
 * downstream operator.
 *
 * @param <T> the deserialized record type.
 */
@PublicEvolving
public final class AckableMessage<T> {

    private final T value;
    private final RocketMQReceiptHandle handle;

    public AckableMessage(T value, RocketMQReceiptHandle handle) {
        this.value = value;
        this.handle = Objects.requireNonNull(handle, "handle should not be null");
    }

    /** The deserialized record value. */
    public T getValue() {
        return value;
    }

    /** The receipt handle used to acknowledge or re-schedule the originating message. */
    public RocketMQReceiptHandle getHandle() {
        return handle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AckableMessage<?> that = (AckableMessage<?>) o;
        return Objects.equals(value, that.value) && handle.equals(that.handle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, handle);
    }

    @Override
    public String toString() {
        return "AckableMessage{value=" + value + ", handle=" + handle + '}';
    }
}
