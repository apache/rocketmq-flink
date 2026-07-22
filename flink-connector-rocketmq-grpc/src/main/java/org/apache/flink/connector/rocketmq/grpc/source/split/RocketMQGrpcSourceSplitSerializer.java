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

package org.apache.flink.connector.rocketmq.grpc.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * The {@link SimpleVersionedSerializer serializer} for the placeholder {@link
 * RocketMQGrpcSourceSplit}. The split carries no data, so serialization is empty.
 */
public class RocketMQGrpcSourceSplitSerializer
        implements SimpleVersionedSerializer<RocketMQGrpcSourceSplit> {

    private static final int CURRENT_VERSION = 2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RocketMQGrpcSourceSplit split) {
        return new byte[0];
    }

    @Override
    public RocketMQGrpcSourceSplit deserialize(int version, byte[] serialized) {
        return RocketMQGrpcSourceSplit.INSTANCE;
    }
}
