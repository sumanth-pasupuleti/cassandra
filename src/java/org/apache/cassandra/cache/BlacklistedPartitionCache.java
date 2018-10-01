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

package org.apache.cassandra.cache;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;

public class BlacklistedPartitionCache
{
    private static final Logger logger = LoggerFactory.getLogger(BlacklistedPartitionCache.class);
    private final AtomicReference<Set<BlacklistedPartition>> blacklistedPartitions = new AtomicReference<>();

    public final static BlacklistedPartitionCache instance = new BlacklistedPartitionCache();

    public BlacklistedPartitionCache()
    {
        //setup a periodic refresh
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(this::refreshCache,
                                                                DatabaseDescriptor.getBlacklistedPartitionsCacheRefreshInMs(),
                                                                DatabaseDescriptor.getBlacklistedPartitionsCacheRefreshInMs(),
                                                                TimeUnit.MILLISECONDS);
    }

    public void refreshCache()
    {
        this.blacklistedPartitions.set(SystemDistributedKeyspace.getBlacklistedPartitions());

        if (DatabaseDescriptor.getBlackListedPartitionsCacheSizeWarnThresholdInMB() > 0)
        {
            //calculate cache size
            long cacheSize = 0;
            for (BlacklistedPartition blacklistedPartition : blacklistedPartitions.get())
            {
                cacheSize += blacklistedPartition.unsharedHeapSize();
            }

            if (cacheSize / (1024 * 1024) >= DatabaseDescriptor.getBlackListedPartitionsCacheSizeWarnThresholdInMB())
            {
                logger.warn(String.format("BlacklistedPartition cache size (%d) MB exceeds threshold size (%d) MB", cacheSize / (1024 * 1024), DatabaseDescriptor.getBlackListedPartitionsCacheSizeWarnThresholdInMB()));
            }
        }
    }

    public boolean contains(TableId tableId, DecoratedKey key)
    {
        return blacklistedPartitions.get().contains(new BlacklistedPartition(tableId, key));
    }

    public int size()
    {
        return blacklistedPartitions.get().size();
    }
}
