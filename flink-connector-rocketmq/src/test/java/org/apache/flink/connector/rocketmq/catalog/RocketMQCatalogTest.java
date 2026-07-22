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
package org.apache.flink.connector.rocketmq.catalog;

import org.apache.flink.connector.rocketmq.common.constant.SchemaRegistryConstant;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.Factory;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocketMQCatalogTest {
    @Mock private SchemaRegistryClient schemaRegistryClient;
    @Mock private DefaultMQAdminExt mqAdminExt;
    @Mock private GetSchemaResponse getSchemaResponse;
    private RocketMQCatalog rocketMQCatalog;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        rocketMQCatalog =
                new RocketMQCatalog(
                        "rocketmq_catalog",
                        "default",
                        "http://127.0.0.1:9876",
                        SchemaRegistryConstant.SCHEMA_REGISTRY_BASE_URL);

        Field schemaRegistryClientField =
                rocketMQCatalog.getClass().getDeclaredField("schemaRegistryClient");
        schemaRegistryClientField.setAccessible(true);
        schemaRegistryClientField.set(rocketMQCatalog, schemaRegistryClient);

        Field mqAdminExtField = rocketMQCatalog.getClass().getDeclaredField("mqAdminExt");
        mqAdminExtField.setAccessible(true);
        mqAdminExtField.set(rocketMQCatalog, mqAdminExt);

        List<String> list = new ArrayList();
        list.add("test");
        Mockito.when(schemaRegistryClient.getSubjectsByTenant("default", "default"))
                .thenReturn(list);

        Mockito.when(mqAdminExt.getNamesrvAddr()).thenReturn("127.0.0.1:9876");
        Mockito.when(schemaRegistryClient.getSchemaBySubject("test")).thenReturn(getSchemaResponse);
        Mockito.when(getSchemaResponse.getType()).thenReturn(SchemaType.AVRO);
        Mockito.when(getSchemaResponse.getIdl())
                .thenReturn(
                        "{\"type\":\"record\",\"name\":\"Charge\","
                                + "\"namespace\":\"org.apache.rocketmq.schema.registry.example.serde\",\"fields\":[{\"name\":\"item\","
                                + "\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}");

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(
                new HashMap<MessageQueue, TopicOffset>(2) {
                    {
                        put(new MessageQueue("test", "default", 0), new TopicOffset());
                        put(new MessageQueue("test", "default", 1), new TopicOffset());
                    }
                });

        Mockito.when(mqAdminExt.examineTopicStats("test")).thenReturn(topicStatsTable);
    }

    @Test
    public void testGetFactory() {
        Optional<Factory> factory = rocketMQCatalog.getFactory();
        assertNotNull(factory.get());
    }

    @Test
    public void testOpen() throws NoSuchFieldException, IllegalAccessException {
        rocketMQCatalog.open();

        Class<? extends RocketMQCatalog> aClass = rocketMQCatalog.getClass();
        Field mqAdminExtField = aClass.getDeclaredField("mqAdminExt");
        mqAdminExtField.setAccessible(true);
        Field schemaRegistryClientField = aClass.getDeclaredField("schemaRegistryClient");
        schemaRegistryClientField.setAccessible(true);

        Object mqAdminExt = mqAdminExtField.get(rocketMQCatalog);
        Object schemaRegistryClient = schemaRegistryClientField.get(rocketMQCatalog);
        assertNotNull(mqAdminExt);
        assertNotNull(schemaRegistryClient);
    }

    @Test
    public void testClose() throws NoSuchFieldException, IllegalAccessException {
        rocketMQCatalog.close();

        Class<? extends RocketMQCatalog> aClass = rocketMQCatalog.getClass();
        Field mqAdminExtField = aClass.getDeclaredField("mqAdminExt");
        mqAdminExtField.setAccessible(true);
        Field schemaRegistryClientField = aClass.getDeclaredField("schemaRegistryClient");
        schemaRegistryClientField.setAccessible(true);

        Object mqAdminExt = mqAdminExtField.get(rocketMQCatalog);
        Object schemaRegistryClient = schemaRegistryClientField.get(rocketMQCatalog);
        assertNull(schemaRegistryClient);
    }

    @Test
    public void testListDatabases() {
        List<String> strings = rocketMQCatalog.listDatabases();
        assertEquals(1, strings.size());
        assertEquals("default", strings.get(0));
    }

    @Test
    public void testGetDatabase() throws DatabaseNotExistException {
        CatalogDatabase database = rocketMQCatalog.getDatabase("default");
        assertNotNull(database);
    }

    @Test
    public void testDatabaseExists() {
        boolean exists = rocketMQCatalog.databaseExists("default");
        assertTrue(exists);
    }

    @Test
    public void testCreateDatabase() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.createDatabase("test", null, false));
    }

    @Test
    public void testDropDatabase() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.dropDatabase("test", false, false));
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> strings = rocketMQCatalog.listTables("default");
        assertEquals(1, strings.size());
        assertEquals("test", strings.get(0));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        ObjectPath objectPath = new ObjectPath("default", "test");
        CatalogBaseTable catalogBaseTable = rocketMQCatalog.getTable(objectPath);
        assertNotNull(catalogBaseTable);
    }

    @Test
    public void testTableExists() {
        ObjectPath objectPath = new ObjectPath("default", "test");
        boolean exists = rocketMQCatalog.tableExists(objectPath);
        assertTrue(exists);
    }

    @Test
    public void testCreateTable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.createTable(null, null, false));
    }

    @Test
    public void testDropTable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.dropTable(null, false));
    }

    @Test
    public void testListFunctions() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.listFunctions("default"));
    }

    @Test
    public void testGetFunction() {
        assertThrows(
                FunctionNotExistException.class,
                () -> {
                    ObjectPath objectPath = new ObjectPath("default", "test");
                    rocketMQCatalog.getFunction(objectPath);
                });
    }

    @Test
    public void testFunctionExists() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.functionExists(null));
    }

    @Test
    public void testCreateFunction() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.createFunction(null, null, false));
    }

    @Test
    public void testAlterFunction() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterFunction(null, null, false));
    }

    @Test
    public void testDropFunction() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.dropFunction(null, false));
    }

    @Test
    public void testAlterDatabase() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterDatabase(null, null, false));
    }

    @Test
    public void testListViews() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.listViews("default"));
    }

    @Test
    public void testAlterTable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterTable(null, null, false));
    }

    @Test
    public void testRenameTable() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.renameTable(null, null, false));
    }

    @Test
    public void testListPartitions() throws TableNotPartitionedException, TableNotExistException {
        List<CatalogPartitionSpec> catalogPartitionSpecs =
                rocketMQCatalog.listPartitions(new ObjectPath("default", "test"));
        assertEquals(2, catalogPartitionSpecs.size());
        assertEquals(
                new ArrayList<CatalogPartitionSpec>() {
                    {
                        add(
                                new CatalogPartitionSpec(
                                        new HashMap<String, String>(1) {
                                            {
                                                put("__queue_id__", String.valueOf(0));
                                            }
                                        }));
                        add(
                                new CatalogPartitionSpec(
                                        new HashMap<String, String>(1) {
                                            {
                                                put("__queue_id__", String.valueOf(1));
                                            }
                                        }));
                    }
                },
                catalogPartitionSpecs);
    }

    @Test
    public void testListPartitionsByFilter() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.listPartitionsByFilter(null, null));
    }

    @Test
    public void testGetPartition() throws PartitionNotExistException {
        ObjectPath objectPath = new ObjectPath("default", "test");
        CatalogPartition partition =
                rocketMQCatalog.getPartition(
                        objectPath,
                        new CatalogPartitionSpec(
                                new HashMap<String, String>(1) {
                                    {
                                        put("__queue_id__", String.valueOf(0));
                                    }
                                }));

        assertEquals(
                new HashMap<String, String>(1) {
                    {
                        put("__queue_id__", String.valueOf(0));
                    }
                },
                partition.getProperties());
    }

    @Test
    public void testPartitionExists() {
        ObjectPath objectPath = new ObjectPath("default", "test");
        boolean test =
                rocketMQCatalog.partitionExists(
                        objectPath,
                        new CatalogPartitionSpec(
                                new HashMap<String, String>(1) {
                                    {
                                        put("__queue_id__", String.valueOf(0));
                                    }
                                }));
        assertNotNull(test);
    }

    @Test
    public void testCreatePartition() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.createPartition(null, null, null, false));
    }

    @Test
    public void testDropPartition() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.dropPartition(null, null, false));
    }

    @Test
    public void testAlterPartition() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterPartition(null, null, null, false));
    }

    @Test
    public void testGetTableStatistics() throws TableNotExistException {
        CatalogTableStatistics statistics = rocketMQCatalog.getTableStatistics(null);
        assertEquals(statistics, CatalogTableStatistics.UNKNOWN);
    }

    @Test
    public void testGetTableColumnStatistics() throws TableNotExistException {
        CatalogColumnStatistics statistics = rocketMQCatalog.getTableColumnStatistics(null);
        assertEquals(statistics, CatalogColumnStatistics.UNKNOWN);
    }

    @Test
    public void testGetPartitionStatistics() throws PartitionNotExistException {
        CatalogTableStatistics statistics = rocketMQCatalog.getPartitionStatistics(null, null);
        assertEquals(statistics, CatalogTableStatistics.UNKNOWN);
    }

    @Test
    public void testGetPartitionColumnStatistics() throws PartitionNotExistException {
        CatalogColumnStatistics statistics =
                rocketMQCatalog.getPartitionColumnStatistics(null, null);
        assertEquals(statistics, CatalogColumnStatistics.UNKNOWN);
    }

    @Test
    public void testAlterTableStatistics() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterTableStatistics(null, null, false));
    }

    @Test
    public void testAlterTableColumnStatistics() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterTableColumnStatistics(null, null, false));
    }

    @Test
    public void testAlterPartitionStatistics() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterPartitionStatistics(null, null, null, false));
    }

    @Test
    public void testAlterPartitionColumnStatistics() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> rocketMQCatalog.alterPartitionColumnStatistics(null, null, null, false));
    }
}
