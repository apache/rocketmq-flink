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

package org.apache.rocketmq.flink.source.table;

import org.apache.rocketmq.flink.common.RocketMQOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
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
import static org.apache.rocketmq.flink.common.RocketMQOptions.CONSUMER_GROUP;
import static org.apache.rocketmq.flink.common.RocketMQOptions.NAME_SERVER_ADDRESS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_ACCESS_KEY;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_COLUMN_ERROR_DEBUG;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_CONSUMER_POLL_MS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_ENCODING;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_END_TIME;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_FIELD_DELIMITER;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_LENGTH_CHECK;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_LINE_DELIMITER;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SECRET_KEY;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SQL;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_TIME;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_TIME_MILLS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_TAG;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_TIME_ZONE;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_USE_NEW_API;
import static org.apache.rocketmq.flink.common.RocketMQOptions.TOPIC;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_LATEST;

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
        requiredOptions.add(TOPIC);
        requiredOptions.add(CONSUMER_GROUP);
        requiredOptions.add(NAME_SERVER_ADDRESS);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(OPTIONAL_TAG);
        optionalOptions.add(OPTIONAL_SQL);
        optionalOptions.add(OPTIONAL_START_MESSAGE_OFFSET);
        optionalOptions.add(OPTIONAL_START_TIME_MILLS);
        optionalOptions.add(OPTIONAL_START_TIME);
        optionalOptions.add(OPTIONAL_END_TIME);
        optionalOptions.add(OPTIONAL_TIME_ZONE);
        optionalOptions.add(OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        optionalOptions.add(OPTIONAL_USE_NEW_API);
        optionalOptions.add(OPTIONAL_ENCODING);
        optionalOptions.add(OPTIONAL_FIELD_DELIMITER);
        optionalOptions.add(OPTIONAL_LINE_DELIMITER);
        optionalOptions.add(OPTIONAL_COLUMN_ERROR_DEBUG);
        optionalOptions.add(OPTIONAL_LENGTH_CHECK);
        optionalOptions.add(OPTIONAL_ACCESS_KEY);
        optionalOptions.add(OPTIONAL_SECRET_KEY);
        optionalOptions.add(OPTIONAL_SCAN_STARTUP_MODE);
        optionalOptions.add(OPTIONAL_CONSUMER_POLL_MS);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();
        Map<String, String> rawProperties = context.getCatalogTable().getOptions();
        Configuration configuration = Configuration.fromMap(rawProperties);
        String topic = configuration.getString(TOPIC);
        String consumerGroup = configuration.getString(CONSUMER_GROUP);
        String nameServerAddress = configuration.getString(NAME_SERVER_ADDRESS);
        String tag = configuration.getString(OPTIONAL_TAG);
        String sql = configuration.getString(OPTIONAL_SQL);
        if (configuration.contains(OPTIONAL_SCAN_STARTUP_MODE)
                && (configuration.contains(OPTIONAL_START_MESSAGE_OFFSET)
                        || configuration.contains(OPTIONAL_START_TIME_MILLS)
                        || configuration.contains(OPTIONAL_START_TIME))) {
            throw new IllegalArgumentException(
                    String.format(
                            "cannot support these configs when %s has been set: [%s, %s, %s] !",
                            OPTIONAL_SCAN_STARTUP_MODE.key(),
                            OPTIONAL_START_MESSAGE_OFFSET.key(),
                            OPTIONAL_START_TIME.key(),
                            OPTIONAL_START_TIME_MILLS.key()));
        }
        long startMessageOffset = configuration.getLong(OPTIONAL_START_MESSAGE_OFFSET);
        long startTimeMs = configuration.getLong(OPTIONAL_START_TIME_MILLS);
        String startDateTime = configuration.getString(OPTIONAL_START_TIME);
        String timeZone = configuration.getString(OPTIONAL_TIME_ZONE);
        String accessKey = configuration.getString(OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(OPTIONAL_SECRET_KEY);
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
        String endDateTime = configuration.getString(OPTIONAL_END_TIME);
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
                configuration.getLong(OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        boolean useNewApi = configuration.getBoolean(OPTIONAL_USE_NEW_API);
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(rawProperties);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        descriptorProperties.putTableSchema("schema", physicalSchema);
        String consumerOffsetMode =
                configuration.getString(
                        RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE, CONSUMER_OFFSET_LATEST);
        long consumerOffsetTimestamp =
                configuration.getLong(
                        RocketMQOptions.OPTIONAL_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis());
        return new RocketMQScanTableSource(
                configuration.getLong(OPTIONAL_CONSUMER_POLL_MS),
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
