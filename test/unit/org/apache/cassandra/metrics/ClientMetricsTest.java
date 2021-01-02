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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class ClientMetricsTest
{
    private static EmbeddedCassandraService embedded;

    @BeforeClass
    public static void setUp() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.authenticator = "PasswordAuthenticator";
        });
        CQLTester.prepareServer();

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();
    }

    @Test
    public void testClientMetrics()
    {
        try (Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                      .withoutJMXReporting()
                                      .withCredentials("cassandra", "cassandra")
                                      .withPort(DatabaseDescriptor.getNativeTransportPort()).build())
        {
            String connectedNativeClientsGauge = "org.apache.cassandra.metrics.Client.ConnectedNativeClients";
            String connectionsGauge = "org.apache.cassandra.metrics.Client.Connections";
            String clientsByProtocolVersionGauge = "org.apache.cassandra.metrics.Client.ClientsByProtocolVersion";
            String authSuccessMeter = "org.apache.cassandra.metrics.Client.AuthSuccess";

            int initialConnectedNativeClients = (int) getGauge(connectedNativeClientsGauge).getValue();
            int initialConnectionsCount = ((ArrayList) getGauge(connectionsGauge).getValue()).size();
            int initialClientsByProtocolVersion = ((ArrayList) getGauge(clientsByProtocolVersionGauge).getValue()).size();
            long initialAuthSuccess = getMeter(authSuccessMeter).getCount();

            try (Session session = cluster.connect())
            {
                int afterConnectedNativeClients = (int) getGauge(connectedNativeClientsGauge).getValue();
                int afterConnectionsCount = ((ArrayList) getGauge(connectionsGauge).getValue()).size();
                int afterClientsByProtocolVersion = ((ArrayList) getGauge(clientsByProtocolVersionGauge).getValue()).size();
                long afterAuthSuccess = getMeter(authSuccessMeter).getCount();


                Assert.assertEquals(2, afterConnectedNativeClients - initialConnectedNativeClients);
                Assert.assertEquals(2, afterConnectionsCount - initialConnectionsCount);
                Assert.assertEquals(1, afterClientsByProtocolVersion - initialClientsByProtocolVersion);
                Assert.assertEquals(2, afterAuthSuccess - initialAuthSuccess);
            }
        }
    }

    private Meter getMeter(String metricName)
    {
        Map<String, Meter> metrics = CassandraMetricsRegistry.Metrics.getMeters((name, metric) -> name.equals(metricName));
        return metrics.get(metricName);
    }

    private Gauge getGauge(String metricName)
    {
        Map<String, Gauge> metrics = CassandraMetricsRegistry.Metrics.getGauges((name, metric) -> name.equals(metricName));
        return metrics.get(metricName);
    }
}
