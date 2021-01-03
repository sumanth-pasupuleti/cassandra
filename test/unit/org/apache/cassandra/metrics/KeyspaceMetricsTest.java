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

package org.apache.cassandra.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class KeyspaceMetricsTest extends SchemaLoader
{
    private static Session session;

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();
    }

    @Test
    public void testWriteMetrics()
    {
        String keyspace = "keyspacemetricstest_write";
        String table = "table1";
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace));
        session.execute(String.format("CREATE TABLE %s.%s (id INT PRIMARY KEY)", keyspace, table));

        KeyspaceMetrics keyspaceMetrics = Keyspace.open(keyspace).metric;
        Assert.assertEquals(0, keyspaceMetrics.writeLatency.totalLatency.getCount());
        Assert.assertEquals(0, (long) keyspaceMetrics.memtableLiveDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.memtableOffHeapDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.memtableOnHeapDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.allMemtablesLiveDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.allMemtablesOffHeapDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.allMemtablesOnHeapDataSize.getValue());
        Assert.assertEquals(0, (long) keyspaceMetrics.memtableColumnsCount.getValue());

        session.execute(String.format("INSERT INTO %s.%s (id) VALUES (1)", keyspace, table));
        Assert.assertTrue(keyspaceMetrics.writeLatency.totalLatency.getCount() > 0);
        Assert.assertTrue(keyspaceMetrics.memtableLiveDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.memtableOffHeapDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.memtableOnHeapDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.allMemtablesLiveDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.allMemtablesOffHeapDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.allMemtablesOnHeapDataSize.getValue() > 0);
        Assert.assertTrue(keyspaceMetrics.memtableColumnsCount.getValue() > 0);

        // cas metrics
        Assert.assertEquals(0, (long) keyspaceMetrics.casPrepare.totalLatency.getCount());
        Assert.assertEquals(0, (long) keyspaceMetrics.casPropose.totalLatency.getCount());
        Assert.assertEquals(0, (long) keyspaceMetrics.casCommit.totalLatency.getCount());
        session.execute(String.format("INSERT INTO %s.%s (id) VALUES (1) IF NOT EXISTS", keyspace, table));
        Assert.assertTrue((long) keyspaceMetrics.casPrepare.totalLatency.getCount() > 0);
        Assert.assertTrue((long) keyspaceMetrics.casPropose.totalLatency.getCount() > 0);
        Assert.assertEquals(0, (long) keyspaceMetrics.casCommit.totalLatency.getCount());
        session.execute(String.format("INSERT INTO %s.%s (id) VALUES (2) IF NOT EXISTS", keyspace, table));
        Assert.assertTrue((long) keyspaceMetrics.casCommit.totalLatency.getCount() > 0);

        session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }

    @Test
    public void testReadMetrics()
    {
        String keyspace = "keyspacemetricstest_read";
        String table = "table1";
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace));
        session.execute(String.format("CREATE TABLE %s.%s (id INT PRIMARY KEY)", keyspace, table));

        KeyspaceMetrics keyspaceMetrics = Keyspace.open(keyspace).metric;
        Assert.assertEquals(0, keyspaceMetrics.readLatency.totalLatency.getCount());
        Assert.assertEquals(0, keyspaceMetrics.sstablesPerReadHistogram.getCount());

        session.execute(String.format("SELECT * FROM %s.%s WHERE id = 1", keyspace, table));
        Assert.assertTrue(keyspaceMetrics.readLatency.totalLatency.getCount() > 0);
        Assert.assertEquals(1, keyspaceMetrics.sstablesPerReadHistogram.getCount());

        session.execute(String.format("DROP KEYSPACE %s;", keyspace));
    }

    @Test
    public void testMetricsCleanupOnDrop()
    {
        String keyspace = "keyspacemetricstest_metrics_cleanup";
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(keyspace));

        // no metrics before creating
        assertEquals(0, metrics.get().count());

        session.execute(String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace));
        // some metrics
        assertTrue(metrics.get().count() > 0);

        session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        // no metrics after drop
        assertEquals(metrics.get().collect(Collectors.joining(",")), 0, metrics.get().count());
    }
    
    @AfterClass
    public static void teardown()
    {
        session.close();
    }
}
