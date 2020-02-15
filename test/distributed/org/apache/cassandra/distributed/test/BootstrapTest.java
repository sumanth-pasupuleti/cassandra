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

package org.apache.cassandra.distributed.test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.impl.NetworkTopology;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class BootstrapTest extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapTest.class);

    @Test
    public void bootstrapTest() throws Throwable
    {
        int originalNodeCount = 2;
        int expandedNodeCount = originalNodeCount + 1;
        Cluster.Builder<IInvokableInstance, Cluster> builder = Cluster.build(originalNodeCount)
                                                                      .withTokenSupplier(Cluster.evenlyDistributedTokens(expandedNodeCount))
                                                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(originalNodeCount, "dc0", "rack0"))
                                                                      .withConfig(config -> config.with(NETWORK, GOSSIP));

        Map<Integer, Long> withBootstrap = null;
        Map<Integer, Long> naturally = null;

        try (Cluster cluster = builder.start())
        {
            populate(cluster);

            InstanceConfig config = builder.withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                           .newInstanceConfig(cluster);
            config.set("auto_bootstrap", true);

            IInstance newInstance = cluster.bootstrap(config);

            new Thread(new Runnable() {

                public void run() {
                    try
                    {
                        logger.info("Hello from thread");
//                        Thread.sleep(Long.parseLong(System.getProperty("cassandra.ring_delay_ms")));
                        Thread.sleep(20000);
                        logger.info("creating ks2");

//                        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + '2' + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': '3'};");
//                        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '2' + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                        String newKeySpace = "distributed_test_keyspace2";
                        cluster.coordinator(1).execute("CREATE KEYSPACE " + newKeySpace + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc0': '3'};", ConsistencyLevel.LOCAL_ONE);
                        cluster.coordinator(1).execute("CREATE TABLE " + newKeySpace + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))", ConsistencyLevel.LOCAL_ONE);

                        logger.info("starting mutations for ks2");
                        for (int i = 0; i < 1000; i++)
                            cluster.coordinator(1).execute("INSERT INTO " + newKeySpace + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                                           ConsistencyLevel.QUORUM,
                                                           i, i, i);

                        logger.info("done with mutations for ks2");
                    }
                    catch (Exception ex)
                    {

                    }
                }
            }).start();

            logger.info("hello from main 1");
            newInstance.startup();
            logger.info("hello from main 2");
            cluster.stream().forEach(instance -> {
                instance.nodetool("cleanup", KEYSPACE, "tbl");
            });

            withBootstrap = count(cluster);
        }

        builder = Cluster.build(expandedNodeCount)
                         .withTokenSupplier(Cluster.evenlyDistributedTokens(expandedNodeCount))
                         .withConfig(config -> config.with(NETWORK, GOSSIP));

        try (Cluster cluster = builder.start())
        {
            populate(cluster);
            naturally = count(cluster);
        }

        Assert.assertEquals(withBootstrap, naturally);
    }

    public void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + 3 + "};");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        for (int i = 0; i < 1000; i++)
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                           ConsistencyLevel.QUORUM,
                                           i, i, i);
    }

    public Map<Integer, Long> count(Cluster cluster)
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .boxed()
                        .collect(Collectors.toMap(nodeId -> nodeId,
                                                  nodeId -> (Long) cluster.get(nodeId).executeInternal("SELECT count(*) FROM " + KEYSPACE + ".tbl")[0][0]));
    }

}
