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

package org.apache.cassandra.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.utils.FBUtilities;

public class StartupClusterConnectivityCheckerTest
{
    private StartupClusterConnectivityChecker localQuorumConnectivityChecker;
    private StartupClusterConnectivityChecker globalQuorumConnectivityChecker;
    private StartupClusterConnectivityChecker noopChecker;
    private StartupClusterConnectivityChecker zeroWaitChecker;

    private static final long TIMEOUT_NANOS = 100;
    private static final int NUM_PER_DC = 6;
    private Set<InetAddress> peers;
    private Set<InetAddress> peersA;
    private Set<InetAddress> peersAMinusLocal;
    private Set<InetAddress> peersB;
    private Set<InetAddress> peersC;

    private String getDatacenter(InetAddress endpoint)
    {
        if (peersA.contains(endpoint))
            return "datacenterA";
        if (peersB.contains(endpoint))
            return "datacenterB";
        else if (peersC.contains(endpoint))
            return "datacenterC";
        return null;
    }

    @Before
    public void setUp() throws UnknownHostException
    {
        localQuorumConnectivityChecker = new StartupClusterConnectivityChecker(TIMEOUT_NANOS, false);
        globalQuorumConnectivityChecker = new StartupClusterConnectivityChecker(TIMEOUT_NANOS, true);
        noopChecker = new StartupClusterConnectivityChecker(-1, false);
        zeroWaitChecker = new StartupClusterConnectivityChecker(0, false);

        peersA = new HashSet<>();
        peersAMinusLocal = new HashSet<>();
        peersA.add(FBUtilities.getBroadcastAddress());

        for (int i = 0; i < NUM_PER_DC - 1; i ++)
        {
            peersA.add(InetAddress.getByName("127.0.1." + i));
            peersAMinusLocal.add(InetAddress.getByName("127.0.1." + i));
        }

        peersB = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i ++)
            peersB.add(InetAddress.getByName("127.0.2." + i));


        peersC = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i ++)
            peersC.add(InetAddress.getByName("127.0.3." + i));

        peers = new HashSet<>();
        peers.addAll(peersA);
        peers.addAll(peersB);
        peers.addAll(peersC);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void execute_HappyPath()
    {
        Sink sink = new Sink(true, true, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertTrue(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_NotAlive()
    {
        Sink sink = new Sink(false, true, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_NoConnectionsAcks()
    {
        Sink sink = new Sink(true, false, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_LocalQuorum()
    {
        // local peer plus 3 peers from same dc shouldn't pass (4/6)
        Set<InetAddress> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 3);
        checkAvailable(localQuorumConnectivityChecker, available, false);

        // local peer plus 4 peers from same dc should pass (5/6)
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        checkAvailable(localQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_GlobalQuorum()
    {
        // local dc passing shouldn't pass globally with two hosts down in datacenterB
        Set<InetAddress> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 2);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, false);

        // All three datacenters should be able to have a single node down
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 1);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, true);

        // Everything being up should work of course
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 1);
        copyCount(peersB, available, NUM_PER_DC);
        copyCount(peersC, available, NUM_PER_DC);
        checkAvailable(globalQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_Noop()
    {
        checkAvailable(noopChecker, new HashSet<>(), true);
    }

    @Test
    public void execute_ZeroWaitHasConnections() throws InterruptedException
    {
        Sink sink = new Sink(true, true, new HashSet<>());
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(zeroWaitChecker.execute(peers, this::getDatacenter));
        MessagingService.instance().clearMessageSinks();
    }

    private void checkAvailable(StartupClusterConnectivityChecker checker, Set<InetAddress> available,
                                boolean shouldPass)
    {
        Sink sink = new Sink(true, true, available);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertEquals(shouldPass, checker.execute(peers, this::getDatacenter));
        MessagingService.instance().clearMessageSinks();
    }

    private void copyCount(Set<InetAddress> source, Set<InetAddress> dest, int count)
    {
        for (InetAddress peer : source)
        {
            if (count <= 0)
                break;

            dest.add(peer);
            count -= 1;
        }
    }

    private static class Sink implements IMessageSink
    {
        private final boolean markAliveInGossip;
        private final boolean processConnectAck;
        private final Set<InetAddress> aliveHosts;
        private final Map<InetAddress, ConnectionTypeRecorder> seenConnectionRequests;

        Sink(boolean markAliveInGossip, boolean processConnectAck, Set<InetAddress> aliveHosts)
        {
            this.markAliveInGossip = markAliveInGossip;
            this.processConnectAck = processConnectAck;
            this.aliveHosts = aliveHosts;
            seenConnectionRequests = new HashMap<>();
        }

        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
        {
            seenConnectionRequests.computeIfAbsent(to, inetAddress ->  new ConnectionTypeRecorder());

            if (!aliveHosts.contains(to))
                return false;

            if (processConnectAck)
            {
                MessageIn messageIn = MessageIn.create(to, null, Collections.emptyMap(), MessagingService.Verb.REQUEST_RESPONSE, MessagingService.current_version);
                MessagingService.instance().getRegisteredCallback(id).callback.response(messageIn);
            }

            if (markAliveInGossip)
                Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(to, new EndpointState(new HeartBeatState(1, 1))));

            return true;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return true;
        }
    }

    private static class ConnectionTypeRecorder
    {
        boolean seenSmallMessageRequest;
        boolean seenLargeMessageRequest;
    }
}
