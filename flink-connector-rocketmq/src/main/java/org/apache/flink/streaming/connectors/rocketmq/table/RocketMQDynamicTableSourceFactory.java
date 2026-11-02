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
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Defines the {@link DynamicTableSourceFactory} implementation to create {@link
 * RocketMQScanTableSource}.
 */
public class RocketMQDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public String factoryIdentifier() {
        return "rocketmq";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(RocketMQSourceOptions.TOPIC);
        requiredOptions.add(RocketMQSourceOptions.CONSUMER_GROUP);
        requiredOptions.add(RocketMQSourceOptions.ENDPOINTS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_TAG);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_SQL);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_SPECIFIC);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_STOP_OFFSET_TIMESTAMP);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_TIME_ZONE);
        optionalOptions.add(RocketMQSourceOptions.PULL_TIMEOUT_MILLIS);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_ENCODING);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_FIELD_DELIMITER);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_LINE_DELIMITER);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_COLUMN_ERROR_DEBUG);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_LENGTH_CHECK);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE);
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE);
        // Kept so existing DDL still validates. Flink 2.x removed SourceFunction, so the FLIP-27
        // source is the only implementation and this switch is a no-op.
        optionalOptions.add(RocketMQSourceOptions.OPTIONAL_USE_NEW_API);
        optionalOptions.add(RocketMQSourceOptions.PULL_BATCH_SIZE);
        optionalOptions.add(RocketMQSourceOptions.ALLOCATE_MESSAGE_QUEUE_STRATEGY);
        optionalOptions.add(RocketMQSourceOptions.AUTO_COMMIT_OFFSET);
        optionalOptions.add(RocketMQSourceOptions.AUTO_COMMIT_OFFSET_INTERVAL);
        optionalOptions.add(RocketMQSourceOptions.ENABLE_MESSAGE_TRACE);
        optionalOptions.add(RocketMQSourceOptions.CUSTOMIZED_TRACE_TOPIC);
        optionalOptions.add(RocketMQOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        optionalOptions.add(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        RequiredOptionValidator.validate(requiredOptions(), context.getCatalogTable().getOptions());
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String topic = configuration.get(RocketMQSourceOptions.TOPIC);
        String consumerGroup = configuration.get(RocketMQSourceOptions.CONSUMER_GROUP);
        String nameServerAddress = configuration.get(RocketMQSourceOptions.ENDPOINTS);
        String tag = configuration.get(RocketMQSourceOptions.OPTIONAL_TAG);
        String sql = configuration.get(RocketMQSourceOptions.OPTIONAL_SQL);
        if (configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE)
                && (configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP)
                        || configuration.contains(
                                RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE))) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot set %s or %s when %s has been set.",
                            RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP.key(),
                            RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE.key(),
                            RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE.key()));
        }
        long startMessageOffset =
                configuration.get(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_SPECIFIC);
        long startTimeMs =
                configuration.get(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP);
        String startDateTime = configuration.get(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE);
        String timeZone = configuration.get(RocketMQSourceOptions.OPTIONAL_TIME_ZONE);
        String accessKey = configuration.get(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.get(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);
        long startTime = startTimeMs;
        if (startTime == -1) {
            if (!StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
                try {
                    startTime = parseDateString(startDateTime, timeZone);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Incorrect datetime format: %s, pls use ISO-8601 "
                                            + "complete date plus hours, minutes and seconds format:%s.",
                                    startDateTime, DATE_FORMAT),
                            e);
                }
            }
        }
        long stopInMs = Long.MAX_VALUE;
        String endDateTime =
                configuration.get(RocketMQSourceOptions.OPTIONAL_STOP_OFFSET_TIMESTAMP);
        if (!StringUtils.isNullOrWhitespaceOnly(endDateTime)) {
            try {
                stopInMs = parseDateString(endDateTime, timeZone);
            } catch (ParseException e) {
                throw new RuntimeException(
                        String.format(
                                "Incorrect datetime format: %s, pls use ISO-8601 "
                                        + "complete date plus hours, minutes and seconds format:%s.",
                                endDateTime, DATE_FORMAT),
                        e);
            }
            Preconditions.checkArgument(
                    stopInMs >= startTime, "Start time should be less than stop time.");
        }
        long partitionDiscoveryIntervalMs =
                configuration.get(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        // Flink 2.x removed TableSchema/SchemaValidator: the deserializer now takes the resolved
        // physical schema directly instead of rebuilding it from the descriptor properties.
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        String consumerOffsetMode =
                configuration.get(
                        RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE,
                        RocketMQConfig.CONSUMER_OFFSET_LATEST);
        long consumerOffsetTimestamp =
                configuration.get(
                        RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP,
                        RocketMQConfig.DEFAULT_START_MESSAGE_OFFSET);

        // Auto-infer scan mode when user sets timestamp but not scan.mode (P0 bug fix)
        if (configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP)
                && !configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE)) {
            consumerOffsetMode = "timestamp";
        }
        return new RocketMQScanTableSource(
                configuration.get(RocketMQSourceOptions.PULL_TIMEOUT_LONG_POLLING_SUSPEND),
                descriptorProperties,
                physicalSchema,
                topic,
                consumerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                sql,
                stopInMs,
                startMessageOffset,
                startMessageOffset < 0 ? startTime : -1L,
                partitionDiscoveryIntervalMs,
                consumerOffsetMode,
                consumerOffsetTimestamp);
    }

    /**
     * Keeps only the physical columns of the catalog schema. Computed and metadata columns are not
     * part of the payload RocketMQ delivers, so they must not reach the deserializer.
     */
    private static ResolvedSchema getPhysicalSchema(ResolvedSchema schema) {
        List<Column> physicalColumns =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList());
        return ResolvedSchema.of(physicalColumns);
    }

    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat =
                FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone(timeZone));
        return simpleDateFormat.parse(dateString).getTime();
    }
}
