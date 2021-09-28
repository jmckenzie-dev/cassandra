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

package org.apache.cassandra.schema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.cql3.QueryProcessor.process;

/**
 * PartitionDenylist uses the system_distributed.partition_denylist table to maintain a list of denylisted partition keys
 * for each keyspace/table.
 *
 * Keys can be entered manually into the partition_denylist table or via the JMX operation StorageProxyMBean.denylistKey
 *
 * The denylist is stored as one CQL partition per table, and the denylisted keys are column names in that partition. The denylisted
 * keys for each table are cached in memory, and reloaded from the partition_denylist table every 24 hours (default) or when the
 * StorageProxyMBean.loadPartitionDenylist is called via JMX.
 *
 * Concurrency of the cache is provided by the concurrency semantics of the guava LoadingCache. All values (DenylistEntry) are
 * immutable collections of keys/tokens which are replaced in whole when the cache refreshes from disk.
 */
public class PartitionDenylist
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionDenylist.class);
    public static final String PARTITION_DENYLIST_TABLE = "partition_denylist";

    private final ExecutorService executor = new ThreadPoolExecutor(2, 2, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    private final LoadingCache<TableId, DenylistEntry> denylist = CacheBuilder.newBuilder()
            .refreshAfterWrite(DatabaseDescriptor.getDenylistRefreshSeconds(), TimeUnit.SECONDS)
            .build(new CacheLoader<TableId, DenylistEntry>()
            {
                @Override
                public DenylistEntry load(final TableId tid) throws Exception
                {
                    return getDenylistForTable(tid);
                }

                @Override
                public ListenableFuture<DenylistEntry> reload(final TableId tid, final DenylistEntry oldValue)
                {
                    ListenableFutureTask<DenylistEntry> task = ListenableFutureTask.create(new Callable<DenylistEntry>()
                    {
                        @Override
                        public DenylistEntry call()
                        {
                            final DenylistEntry newEntry = getDenylistForTable(tid);
                            if (newEntry != null)
                                return newEntry;
                            if (oldValue != null)
                                return oldValue;
                            return new DenylistEntry();
                        }
                    });
                    executor.execute(task);
                    return task;
                }
            });

    /**
     * Denylist entry is never mutated once constructed, only replaced with a new entry when the cache is refreshed
     */
    private static class DenylistEntry
    {
        public final ImmutableSet<ByteBuffer> keys;
        public final ImmutableSortedSet<Token> tokens;

        public DenylistEntry()
        {
            keys = ImmutableSet.of();
            tokens = ImmutableSortedSet.of();
        }

        public DenylistEntry(final ImmutableSet<ByteBuffer> keys, final ImmutableSortedSet<Token> tokens)
        {
            this.keys = keys;
            this.tokens = tokens;
        }
    }

    // synchronized on this
    private int loadAttempts = 0;
    private int loadSuccesses = 0;

    public synchronized int getLoadAttempts()
    {
        return loadAttempts;
    }
    public synchronized int getLoadSuccesses()
    {
        return loadSuccesses;
    }

    /**
     * Performs initial load of the partition denylist.  Should be called at startup and only loads if the operation
     * is expected to succeed.  If it is not possible to load at call time, a timer is set to retry.
     */
    public void initialLoad()
    {
        if (!DatabaseDescriptor.enablePartitionDenylist())
            return;

        synchronized (this)
        {
            loadAttempts++;
        }

        // Check if there are sufficient nodes to attempt reading all the denylist partitions before issuing the query.
        // The pre-check prevents definite range-slice unavailables being marked and triggering an alert.  Nodes may still change
        // state between the check and the query, but it should significantly reduce the alert volume.
        String retryReason = "Insufficient nodes";
        try
        {
            if (readAllHasSufficientNodes() && load())
            {
                return;
            }
        }
        catch (Throwable tr)
        {
            logger.error("Failed to load partition denylist", tr);
            retryReason = "Exception";
        }

        // This path will also be taken on other failures other than UnavailableException,
        // but seems like a good idea to retry anyway.
        int retryInSeconds = DatabaseDescriptor.getDenylistInitialLoadRetrySeconds();
        logger.info(retryReason + " while loading partition denylist cache.  Scheduled retry in {} seconds.",
                    retryInSeconds);
        ScheduledExecutors.optionalTasks.schedule(this::initialLoad, retryInSeconds, TimeUnit.SECONDS);
    }

    private boolean readAllHasSufficientNodes()
    {
        return RangeCommands.sufficientLiveNodesForSelectStar(SystemDistributedKeyspace.PartitionDenylistTable,
                                                              DatabaseDescriptor.getDenylistConsistencyLevel());
    }

    public boolean load()
    {
        final long start = System.currentTimeMillis();
        final Map<TableId, DenylistEntry> allDenylists = getDenylistForAllTables();
        if (allDenylists == null)
            return false;

        // Cache iterators are weakly-consistent
        // Note: we explicitly don't clear the cache and repopulate it as we don't want a window of time where we
        // are not denying partitions we need to deny, and it's also simplest to just idempotently replace / add to
        // the existing cache with whatever new records may have been added.
        for (final Iterator<TableId> it = denylist.asMap().keySet().iterator(); it.hasNext(); )
        {
            final TableId tid = it.next();
            if (!allDenylists.containsKey(tid))
                allDenylists.put(tid, new DenylistEntry());
        }
        denylist.putAll(allDenylists);
        logger.info("Loaded partition denylist cache in {}ms", System.currentTimeMillis() - start);

        synchronized (this)
        {
            loadSuccesses++;
        }
        return true;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table
     */
    public boolean addKeyToDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        if (!isPermitted(keyspace))
            return false;

        final byte[] keyBytes = new byte[key.remaining()];
        key.slice().get(keyBytes);

        final String insert = String.format("INSERT INTO system_distributed.partition_denylist (ks_name, cf_name, key) VALUES ('%s', '%s', 0x%s)",
                                            keyspace, table, Hex.bytesToHex(keyBytes));

        try
        {
            process(insert, DatabaseDescriptor.getDenylistConsistencyLevel());
            return true;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to denylist key [{}] in {}/{}", Hex.bytesToHex(keyBytes), keyspace, table, e);
        }
        return false;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table
     */
    public boolean removeKeyFromDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        final byte[] keyBytes = new byte[key.remaining()];
        key.slice().get(keyBytes);

        final String delete = String.format("DELETE FROM system_distributed.partition_denylist " +
                                            "WHERE ks_name = '%s' " +
                                            "AND cf_name = '%s' " +
                                            "AND key = 0x%s",
                                            keyspace, table, Hex.bytesToHex(keyBytes));

        try
        {
            process(delete, DatabaseDescriptor.getDenylistConsistencyLevel());
            return true;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to remove key from denylist: [{}] in {}/{}", Hex.bytesToHex(keyBytes), keyspace, table, e);
        }
        return false;
    }

    /**
     * We disallow denylisting partitions in certain critical keyspaces to prevent users from making their clusters
     * inoperable.
     */
    private boolean isPermitted(final String keyspace)
    {
        return !SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.TRACE_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.AUTH_KEYSPACE_NAME.equals(keyspace);
    }

    public boolean isKeyPermitted(final String keyspace, final String table, final ByteBuffer key)
    {
        return isKeyPermitted(getTableId(keyspace, table), key);
    }

    public boolean isKeyPermitted(final TableId tid, final ByteBuffer key)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.enablePartitionDenylist() || tid == null || !isPermitted(tmd.keyspace))
            return true;

        try
        {
            return !denylist.get(tid).keys.contains(key);
        }
        catch (final ExecutionException e)
        {
            // In the event of an error accessing or populating the cache, assume it's not denylisted
            logAccessFailure(tmd, e);
            return true;
        }
    }

    private void logAccessFailure(final TableMetadata tmd, Throwable e)
    {
        if (tmd == null)
            logger.debug("Failed to access partition denylist cache for unknown table id {}", tmd.id, e);
        else
            logger.debug("Failed to access partition denylist cache for {}/{}", tmd.keyspace, tmd.name, e);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRange(final String keyspace, final String table, final AbstractBounds<PartitionPosition> range)
    {
        return getDeniedKeysInRange(getTableId(keyspace, table), range);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRange(final TableId tid, final AbstractBounds<PartitionPosition> range)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.enablePartitionDenylist() || tid == null || !isPermitted(tmd.keyspace))
            return 0;

        try
        {
            final DenylistEntry denylistEntry = denylist.get(tid);
            if (denylistEntry.tokens.size() == 0)
                return 0;
            final Token startToken = range.left.getToken();
            final Token endToken = range.right.getToken();

            // Normal case
            if (startToken.compareTo(endToken) <= 0 || endToken.isMinimum())
            {
                NavigableSet<Token> subSet = denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind()));
                if (!endToken.isMinimum())
                    subSet = subSet.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind()));
                return subSet.size();
            }

            // Wrap around case
            return denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind())).size()
                   + denylistEntry.tokens.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind())).size();
        }
        catch (final ExecutionException e)
        {
            logAccessFailure(tmd, e);
            return 0;
        }
    }

    private DenylistEntry getDenylistForTable(final TableId tid)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            return new DenylistEntry();

        // We pull max keys + 1 in order to check below whether we've surpassed the allowable limit or not
        final String readDenylist = String.format("SELECT * FROM %s.%s WHERE ks_name='%s' AND cf_name='%s' LIMIT %d", SystemDistributedKeyspace.NAME, PARTITION_DENYLIST_TABLE,
                                                  tmd.keyspace, tmd.name, DatabaseDescriptor.maxDenylistKeysPerTable() + 1);

        try
        {
            final UntypedResultSet results = process(readDenylist, DatabaseDescriptor.getDenylistConsistencyLevel());
            if (results == null || results.isEmpty())
                return new DenylistEntry();

            if (results.size() > DatabaseDescriptor.maxDenylistKeysPerTable())
                logger.error("Partition denylist for {}/{} has exceeded the maximum allowable size ({}). Remaining keys were ignored; please reduce the number of keys denied or increase the size of your cache to avoid inconsistency in denied partitions across nodes.", tmd.keyspace, tmd.name, DatabaseDescriptor.maxDenylistKeysPerTable());

            final Set<ByteBuffer> keys = new HashSet<>();
            final NavigableSet<Token> tokens = new TreeSet<>();
            for (final UntypedResultSet.Row row : results)
            {
                final ByteBuffer key = row.getBlob("key");
                keys.add(key);
                tokens.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }
            return new DenylistEntry(ImmutableSet.copyOf(keys), ImmutableSortedSet.copyOf(tokens));
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_denylist table for {}/{}. Returning empty list.", tmd.keyspace, tmd.name, e);
            return new DenylistEntry();
        }
    }

    private Map<TableId, DenylistEntry> getDenylistForAllTables()
    {
        // We pull max keys + 1 in order to check below whether we've surpassed the allowable limit or not
        final String readDenylist = String.format("SELECT * FROM %s.%s LIMIT %d", SystemDistributedKeyspace.NAME, PARTITION_DENYLIST_TABLE, DatabaseDescriptor.maxDenylistKeysTotal() + 1);
        try
        {
            final UntypedResultSet results = process(readDenylist, DatabaseDescriptor.getDenylistConsistencyLevel());
            if (results == null || results.isEmpty())
                return new HashMap<>();

            if (results.size() > DatabaseDescriptor.maxDenylistKeysTotal())
                logger.error("Partition denylist has exceeded the maximum allowable total size ({}). Remaining keys were ignored; please reduce the number of keys denied or increase the size of your cache to avoid inconsistency in denied partitions across nodes.", DatabaseDescriptor.maxDenylistKeysTotal());

            final Map<TableId, Pair<Set<ByteBuffer>, NavigableSet<Token>>> allDenylists = new HashMap<>();
            for (final UntypedResultSet.Row row : results)
            {
                final TableId tid = getTableId(row.getString("ks_name"), row.getString("cf_name"));
                if (tid == null)
                    continue;

                Pair<Set<ByteBuffer>, NavigableSet<Token>> pair =
                    allDenylists.computeIfAbsent(tid, k -> Pair.create(new HashSet<>(), new TreeSet<>()));

                final ByteBuffer key = row.getBlob("key");
                pair.left.add(key);
                pair.right.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }

            final Map<TableId, DenylistEntry> ret = new HashMap<>();
            for (final TableId tid : allDenylists.keySet())
            {
                final Pair<Set<ByteBuffer>, NavigableSet<Token>> entries = allDenylists.get(tid);
                ret.put(tid, new DenylistEntry(ImmutableSet.copyOf(entries.left), ImmutableSortedSet.copyOf(entries.right)));
            }
            return ret;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_denylist table on startup", e);
            return null;
        }
    }

    private TableId getTableId(final String keyspace, final String table)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(keyspace, table);
        return tmd == null ? null : tmd.id;
    }
}