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

package org.apache.flink.connector.rocketmq.grpc.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.grpc.RocketMQGrpcOptions;
import org.apache.flink.connector.rocketmq.grpc.sink.RocketMQGrpcSinkOptions;
import org.apache.flink.connector.rocketmq.grpc.source.RocketMQGrpcSourceOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link DynamicTableSourceFactory} and {@link DynamicTableSinkFactory} for the RocketMQ gRPC
 * connector. It is registered under the {@code rocketmq-grpc} identifier.
 */
@Internal
public class RocketMQGrpcDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "rocketmq-grpc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RocketMQGrpcConnectorOptions.ENDPOINTS);
        options.add(RocketMQGrpcConnectorOptions.TOPIC);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RocketMQGrpcConnectorOptions.NAMESPACE);
        options.add(RocketMQGrpcConnectorOptions.ACCESS_KEY);
        options.add(RocketMQGrpcConnectorOptions.SECRET_KEY);
        options.add(RocketMQGrpcConnectorOptions.CREDENTIALS_RESOLVER_CLASS);
        options.add(RocketMQGrpcConnectorOptions.TLS_ENABLED);
        options.add(RocketMQGrpcConnectorOptions.REQUEST_TIMEOUT);
        options.add(RocketMQGrpcConnectorOptions.LITE_TOPIC);
        options.add(RocketMQGrpcConnectorOptions.CONSUMER_GROUP);
        options.add(RocketMQGrpcConnectorOptions.AWAIT_DURATION);
        options.add(RocketMQGrpcConnectorOptions.INVISIBLE_DURATION);
        options.add(RocketMQGrpcConnectorOptions.MAX_MESSAGE_NUM);
        options.add(RocketMQGrpcConnectorOptions.FETCH_CONCURRENCY);
        options.add(RocketMQGrpcConnectorOptions.RENEWAL_POLICY_CLASS);
        options.add(RocketMQGrpcConnectorOptions.RENEWAL_AHEAD_TIME);
        options.add(RocketMQGrpcConnectorOptions.MAX_ATTEMPTS);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        checkNotNull(
                options.get(RocketMQGrpcConnectorOptions.CONSUMER_GROUP),
                "'%s' is required for the RocketMQ gRPC table source.",
                RocketMQGrpcConnectorOptions.CONSUMER_GROUP.key());

        final DataType physicalDataType = context.getPhysicalRowDataType();
        return new RocketMQGrpcDynamicTableSource(
                buildConfiguration(options),
                options.get(RocketMQGrpcConnectorOptions.TOPIC),
                Boundedness.CONTINUOUS_UNBOUNDED,
                physicalDataType,
                decodingFormat);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final String liteTopic = options.get(RocketMQGrpcConnectorOptions.LITE_TOPIC);
        checkNotNull(
                liteTopic,
                "'%s' is required for the RocketMQ gRPC table sink.",
                RocketMQGrpcConnectorOptions.LITE_TOPIC.key());
        final DataType physicalDataType = context.getPhysicalRowDataType();
        return new RocketMQGrpcDynamicTableSink(
                buildConfiguration(options),
                options.get(RocketMQGrpcConnectorOptions.TOPIC),
                liteTopic,
                physicalDataType,
                encodingFormat);
    }

    /**
     * Translates the user-facing SQL DDL options ({@link RocketMQGrpcConnectorOptions}) into a
     * {@link Configuration} keyed by the programmatic/SDK options that the source and sink builders
     * and their runtime read.
     */
    private static Configuration buildConfiguration(ReadableConfig options) {
        final Configuration configuration = new Configuration();
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.ENDPOINTS,
                RocketMQGrpcOptions.ENDPOINTS);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.NAMESPACE,
                RocketMQGrpcOptions.NAMESPACE);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.ACCESS_KEY,
                RocketMQGrpcOptions.ACCESS_KEY);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.SECRET_KEY,
                RocketMQGrpcOptions.SECRET_KEY);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.CREDENTIALS_RESOLVER_CLASS,
                RocketMQGrpcOptions.CREDENTIALS_RESOLVER_CLASS);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.TLS_ENABLED,
                RocketMQGrpcOptions.TLS_ENABLED);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.REQUEST_TIMEOUT,
                RocketMQGrpcOptions.REQUEST_TIMEOUT);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.TOPIC,
                RocketMQGrpcSinkOptions.TOPIC);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.LITE_TOPIC,
                RocketMQGrpcSinkOptions.LITE_TOPIC);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.CONSUMER_GROUP,
                RocketMQGrpcSourceOptions.CONSUMER_GROUP);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.AWAIT_DURATION,
                RocketMQGrpcSourceOptions.AWAIT_DURATION);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.INVISIBLE_DURATION,
                RocketMQGrpcSourceOptions.INVISIBLE_DURATION);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.MAX_MESSAGE_NUM,
                RocketMQGrpcSourceOptions.MAX_MESSAGE_NUM);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.FETCH_CONCURRENCY,
                RocketMQGrpcSourceOptions.FETCH_CONCURRENCY);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.RENEWAL_POLICY_CLASS,
                RocketMQGrpcSourceOptions.RENEWAL_POLICY_CLASS);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.RENEWAL_AHEAD_TIME,
                RocketMQGrpcSourceOptions.RENEWAL_AHEAD_TIME);
        copyMapped(
                options,
                configuration,
                RocketMQGrpcConnectorOptions.MAX_ATTEMPTS,
                RocketMQGrpcSinkOptions.MAX_ATTEMPTS);
        return configuration;
    }

    private static <T> void copyMapped(
            ReadableConfig source,
            Configuration target,
            ConfigOption<T> sourceOption,
            ConfigOption<T> targetOption) {
        final T value = source.get(sourceOption);
        if (value != null) {
            target.set(targetOption, value);
        }
    }
}
