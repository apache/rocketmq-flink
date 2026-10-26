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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link RocketMQDynamicTableSourceFactory}. */
public class RocketMQDynamicTableSourceFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Collections.singletonList(Column.physical("name", STRING().notNull())),
                    new ArrayList<>(),
                    null);

    private static final String IDENTIFIER = "rocketmq";
    private static final String TOPIC = "test_source";
    private static final String CONSUMER_GROUP = "test_consumer";
    private static final String NAME_SERVER_ADDRESS = "127.0.0.1:9876";

    /**
     * A missing required option must be reported by key only. Note that this drives the factory
     * directly: going through {@link FactoryUtil#createTableSource} lets Flink run its own required
     * option check first, and that message repeats every table option, including the access key in
     * plaintext.
     */
    @Test
    public void testMissingRequiredOptionDoesNotEchoOptionValues() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", IDENTIFIER);
        options.put(RocketMQSourceOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQSourceOptions.CONSUMER_GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY.key(), "ak-value-must-not-leak");
        options.put(RocketMQSourceOptions.OPTIONAL_SECRET_KEY.key(), "sk-value-must-not-leak");

        final ResolvedCatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(SCHEMA).build(),
                                "mock source",
                                Collections.emptyList(),
                                options),
                        SCHEMA);
        final DynamicTableFactory.Context context =
                new DynamicTableFactory.Context() {
                    @Override
                    public ObjectIdentifier getObjectIdentifier() {
                        return ObjectIdentifier.of("default", "default", IDENTIFIER);
                    }

                    @Override
                    public ResolvedCatalogTable getCatalogTable() {
                        return table;
                    }

                    @Override
                    public Configuration getConfiguration() {
                        return new Configuration();
                    }

                    @Override
                    public ClassLoader getClassLoader() {
                        return RocketMQDynamicTableSourceFactoryTest.class.getClassLoader();
                    }

                    @Override
                    public boolean isTemporary() {
                        return false;
                    }
                };

        try {
            new RocketMQDynamicTableSourceFactory().createDynamicTableSource(context);
            fail("expected the missing endpoints option to be reported");
        } catch (ValidationException e) {
            final String message = e.getMessage();
            assertTrue(message, message.contains(RocketMQSourceOptions.ENDPOINTS.key()));
            assertFalse(message, message.contains("ak-value-must-not-leak"));
            assertFalse(message, message.contains("sk-value-must-not-leak"));
        }
    }

    @Test
    public void testRocketMQDynamicTableSourceWithLegalOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", IDENTIFIER);
        options.put(RocketMQSourceOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQSourceOptions.CONSUMER_GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQSourceOptions.ENDPOINTS.key(), NAME_SERVER_ADDRESS);
        options.put(
                RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP.key(),
                String.valueOf(System.currentTimeMillis()));
        final DynamicTableSource tableSource = createTableSource(options);
        assertTrue(tableSource instanceof RocketMQScanTableSource);
        assertEquals(RocketMQScanTableSource.class.getName(), tableSource.asSummaryString());
    }

    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSourceWithoutRequiredOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", IDENTIFIER);
        options.put(RocketMQSourceOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQSourceOptions.CONSUMER_GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQSourceOptions.OPTIONAL_TAG.key(), "test_tag");
        createTableSource(options);
    }

    @Test(expected = ValidationException.class)
    public void testRocketMQDynamicTableSourceWithUnknownOption() {
        final Map<String, String> options = new HashMap<>();
        options.put(RocketMQSourceOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQSourceOptions.CONSUMER_GROUP.key(), CONSUMER_GROUP);
        // options.put(RocketMQSourceOptions.PERSIST_OFFSET_INTERVAL.key(), NAME_SERVER_ADDRESS);
        options.put("unknown", "test_option");
        createTableSource(options);
    }

    @Test
    public void testRocketMQDynamicTableSourceWithSql() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", IDENTIFIER);
        options.put(RocketMQSourceOptions.TOPIC.key(), TOPIC);
        options.put(RocketMQSourceOptions.CONSUMER_GROUP.key(), CONSUMER_GROUP);
        options.put(RocketMQSourceOptions.ENDPOINTS.key(), NAME_SERVER_ADDRESS);
        options.put(
                RocketMQSourceOptions.OPTIONAL_STARTUP_OFFSET_TIMESTAMP.key(),
                String.valueOf(System.currentTimeMillis()));
        options.put(
                RocketMQSourceOptions.OPTIONAL_SQL.key(),
                "(TAGS is not null and TAGS in ('TagA', 'TagB'))");
        final DynamicTableSource tableSource = createTableSource(options);
        assertTrue(tableSource instanceof RocketMQScanTableSource);
        assertEquals(RocketMQScanTableSource.class.getName(), tableSource.asSummaryString());
    }

    private static DynamicTableSource createTableSource(
            Map<String, String> options, Configuration conf) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", IDENTIFIER),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(SCHEMA).build(),
                                "mock source",
                                Collections.emptyList(),
                                options),
                        SCHEMA),
                conf,
                RocketMQDynamicTableSourceFactory.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(options, new Configuration());
    }
}
