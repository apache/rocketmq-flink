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

package org.apache.flink.connector.rocketmq.source.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.legacy.RocketMQConfig;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

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
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String topic = configuration.getString(RocketMQSourceOptions.TOPIC);
        String consumerGroup = configuration.getString(RocketMQSourceOptions.CONSUMER_GROUP);
        String nameServerAddress = configuration.getString(RocketMQSourceOptions.ENDPOINTS);
        String tag = configuration.getString(RocketMQSourceOptions.OPTIONAL_TAG);
        String sql = configuration.getString(RocketMQSourceOptions.OPTIONAL_SQL);
        if (configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE)
                && (configuration.contains(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP)
                        || configuration.contains(
                                RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE))) {
            throw new IllegalArgumentException(
                    String.format(
                            "cannot support these configs when %s has been set: [%s] !",
                            RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE.key(),
                            RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_SPECIFIC.key()));
        }
        long startMessageOffset =
                configuration.getLong(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_SPECIFIC);
        long startTimeMs =
                configuration.getLong(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP);
        String startDateTime =
                configuration.getString(RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_DATE);
        String timeZone = configuration.getString(RocketMQSourceOptions.OPTIONAL_TIME_ZONE);
        String accessKey = configuration.getString(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);
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
                configuration.getString(RocketMQSourceOptions.OPTIONAL_STOP_OFFSET_TIMESTAMP);
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
                configuration.getLong(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        boolean useNewApi = configuration.getBoolean(RocketMQSourceOptions.OPTIONAL_USE_NEW_API);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        descriptorProperties.putTableSchema("schema", physicalSchema);
        String consumerOffsetMode =
                configuration.getString(
                        RocketMQSourceOptions.OPTIONAL_STARTUP_SCAN_MODE,
                        RocketMQConfig.CONSUMER_OFFSET_LATEST);
        long consumerOffsetTimestamp =
                configuration.getLong(
                        RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP,
                        System.currentTimeMillis());
        return new RocketMQScanTableSource(
                configuration.getLong(RocketMQSourceOptions.PULL_TIMEOUT_LONG_POLLING_SUSPEND),
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
                consumerOffsetTimestamp,
                useNewApi);
    }

    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat =
                FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone(timeZone));
        return simpleDateFormat.parse(dateString).getTime();
    }
}
