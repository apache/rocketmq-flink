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

package org.apache.flink.streaming.connectors.rocketmq.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.common.config.RocketMQOptions.ENDPOINTS;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_ACCESS_KEY;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_ENCODING;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_FIELD_DELIMITER;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_SECRET_KEY;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_IS_DYNAMIC_TAG;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_KEYS_TO_BODY;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_KEY_COLUMNS;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_RETRY_TIMES;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.OPTIONAL_WRITE_SLEEP_TIME_MS;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.PRODUCER_GROUP;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.TAG;
import static org.apache.flink.connector.rocketmq.sink.RocketMQSinkOptions.TOPIC;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSinkFactory} implementation to create {@link
 * RocketMQDynamicTableSink}.
 */
public class RocketMQDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "rocketmq";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(TOPIC);
        requiredOptions.add(PRODUCER_GROUP);
        requiredOptions.add(ENDPOINTS);

        // requiredOptions.add(PERSIST_OFFSET_INTERVAL);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(TAG);
        optionalOptions.add(OPTIONAL_ENCODING);
        optionalOptions.add(OPTIONAL_FIELD_DELIMITER);
        optionalOptions.add(OPTIONAL_ACCESS_KEY);
        optionalOptions.add(OPTIONAL_SECRET_KEY);
        optionalOptions.add(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
        optionalOptions.add(OPTIONAL_WRITE_RETRY_TIMES);
        optionalOptions.add(OPTIONAL_WRITE_SLEEP_TIME_MS);
        optionalOptions.add(OPTIONAL_WRITE_IS_DYNAMIC_TAG);
        optionalOptions.add(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED);
        optionalOptions.add(OPTIONAL_WRITE_KEYS_TO_BODY);
        optionalOptions.add(OPTIONAL_WRITE_KEY_COLUMNS);
        optionalOptions.add(RocketMQSinkOptions.DELIVERY_GUARANTEE);
        optionalOptions.add(RocketMQSinkOptions.SEND_RETRY_TIMES);
        optionalOptions.add(RocketMQSinkOptions.SEND_TIMEOUT);
        optionalOptions.add(RocketMQSinkOptions.SEND_PENDING_MAX);
        optionalOptions.add(RocketMQSinkOptions.EXECUTOR_NUM);
        optionalOptions.add(RocketMQSinkOptions.TRANSACTION_TIMEOUT);
        return optionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        RequiredOptionValidator.validate(requiredOptions(), context.getCatalogTable().getOptions());
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration properties = Configuration.fromMap(rawProperties);
        String topicName = properties.get(TOPIC);
        String producerGroup = properties.get(PRODUCER_GROUP);
        String nameServerAddress = properties.get(ENDPOINTS);
        String tag = properties.get(TAG);
        String accessKey = properties.get(OPTIONAL_ACCESS_KEY);
        String secretKey = properties.get(OPTIONAL_SECRET_KEY);
        String dynamicColumn = properties.get(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN);
        String encoding = properties.get(OPTIONAL_ENCODING);
        String fieldDelimiter = properties.get(OPTIONAL_FIELD_DELIMITER);
        long sleepTimeMs = properties.get(OPTIONAL_WRITE_SLEEP_TIME_MS);
        boolean isDynamicTag = properties.get(OPTIONAL_WRITE_IS_DYNAMIC_TAG);
        boolean isDynamicTagIncluded =
                properties.get(OPTIONAL_WRITE_DYNAMIC_TAG_COLUMN_WRITE_INCLUDED);
        boolean writeKeysToBody = properties.get(OPTIONAL_WRITE_KEYS_TO_BODY);
        String keyColumnsConfig = properties.get(OPTIONAL_WRITE_KEY_COLUMNS);
        String[] keyColumns = new String[0];
        if (keyColumnsConfig != null && keyColumnsConfig.length() > 0) {
            keyColumns = keyColumnsConfig.split(",");
        }
        String deliveryGuarantee = properties.get(RocketMQSinkOptions.DELIVERY_GUARANTEE);
        // `retry.times` is the legacy DDL name of the send retry count. The writer only reads
        // SEND_RETRY_TIMES, so honour the legacy key when the new one is absent instead of
        // silently dropping it; with neither set the documented default (3) applies.
        int sendRetryTimes =
                properties
                        .getOptional(RocketMQSinkOptions.SEND_RETRY_TIMES)
                        .orElseGet(
                                () ->
                                        properties
                                                .getOptional(OPTIONAL_WRITE_RETRY_TIMES)
                                                .orElseGet(
                                                        () ->
                                                                RocketMQSinkOptions.SEND_RETRY_TIMES
                                                                        .defaultValue()));
        long sendTimeout = properties.get(RocketMQSinkOptions.SEND_TIMEOUT);
        int sendPendingMax = properties.get(RocketMQSinkOptions.SEND_PENDING_MAX);
        int executorNum = properties.get(RocketMQSinkOptions.EXECUTOR_NUM);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        return new RocketMQDynamicTableSink(
                descriptorProperties,
                physicalSchema,
                topicName,
                producerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                dynamicColumn,
                fieldDelimiter,
                encoding,
                sleepTimeMs,
                isDynamicTag,
                isDynamicTagIncluded,
                writeKeysToBody,
                keyColumns,
                deliveryGuarantee,
                sendRetryTimes,
                sendTimeout,
                sendPendingMax,
                executorNum);
    }

    /**
     * Keeps only the physical columns of the catalog schema. Computed and metadata columns are not
     * written to the RocketMQ message body, so they must not reach the row converter.
     */
    private static ResolvedSchema getPhysicalSchema(ResolvedSchema schema) {
        List<Column> physicalColumns =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());
        return ResolvedSchema.of(physicalColumns);
    }
}
