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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class SystemKeyspaceStorage implements LogStorage
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspaceStorage.class);
    public static final String NAME =  org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     * <p>
     * gen 0: original definition in 6.0
     */
    public static final long GENERATION = 0;

    private static final int TAIL_SIZE = 1024;
    // An Entry keeps its parsed statement, so bound the tail by serialized size too
    private static final long TAIL_MAX_BYTES = 16 * 1024 * 1024;

    private final Supplier<MetadataSnapshots> snapshots;
    private final long tailMaxBytes;

    /**
     * The most recent entries this node appended, keyed by epoch and always a contiguous run, so that a committing
     * client catching up a few epochs does not re-read and re-parse them (an AlterSchema entry re-parses its CQL).
     * Empty after a restart. Sharing the Entry is safe: after commit, an AlterSchema is only read (its CQL and
     * execution timestamp are serialized, apply() does not write it), and readers never modify entries.
     */
    private final ConcurrentSkipListMap<Long, WeightedEntry> tail = new ConcurrentSkipListMap<>();
    private long tailBytes; // guarded by tail; readers do not lock

    private static final class WeightedEntry
    {
        final Entry entry;
        final int bytes;

        WeightedEntry(Entry entry, int bytes)
        {
            this.entry = entry;
            this.bytes = bytes;
        }
    }

    public SystemKeyspaceStorage()
    {
        this(() -> ClusterMetadataService.instance().snapshotManager());
    }

    @VisibleForTesting
    public SystemKeyspaceStorage(Supplier<MetadataSnapshots> snapshots)
    {
        this(snapshots, TAIL_MAX_BYTES);
    }

    @VisibleForTesting
    SystemKeyspaceStorage(Supplier<MetadataSnapshots> snapshots, long tailMaxBytes)
    {
        this.snapshots = snapshots;
        this.tailMaxBytes = tailMaxBytes;
    }

    // This method is always called from a single thread, so doesn't have to be synchonised.
    public void append(Entry entry)
    {
        try
        {
            // TODO get lowest supported metadata version from ClusterMetadata
            ByteBuffer serializedTransformation = entry.transform.kind().toVersionedBytes(entry.transform);
            String query = String.format("INSERT INTO %s.%s (epoch, entry_id, transformation, kind) VALUES (?,?,?,?)",
                                         SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);
            executeInternal(query, entry.epoch.getEpoch(), entry.id.entryId, serializedTransformation, entry.transform.kind().id);
            remember(entry, serializedTransformation.remaining());
            // todo; should probably not flush every time, but it simplifies tests
            Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(NAME).forceBlockingFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
        }
        catch (Throwable t)
        {
            logger.error("Could not persist the entry {} proceeding with in-memory commit.", entry, t);
        }
    }

    private void remember(Entry entry, int bytes)
    {
        synchronized (tail)
        {
            long epoch = entry.epoch.getEpoch();
            Map.Entry<Long, WeightedEntry> last = tail.lastEntry();
            // A gap (e.g. after a ForceSnapshot) ends the contiguous run
            if (last != null && last.getKey() + 1 != epoch)
                clearTail();
            if (bytes > tailMaxBytes)
            {
                // Not kept, so the run would have a hole: drop it and let readers use the table
                clearTail();
                return;
            }
            tail.put(epoch, new WeightedEntry(entry, bytes));
            tailBytes += bytes;
            while (epoch - tail.firstKey() >= TAIL_SIZE || tailBytes > tailMaxBytes)
                tailBytes -= tail.pollFirstEntry().getValue().bytes;
        }
    }

    private void clearTail()
    {
        synchronized (tail)
        {
            tail.clear();
            tailBytes = 0;
        }
    }

    @VisibleForTesting
    long tailBytes()
    {
        synchronized (tail)
        {
            return tailBytes;
        }
    }

    public synchronized static boolean hasAnyEpoch()
    {
        String query = String.format("SELECT epoch, kind FROM %s.%s LIMIT 2", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);

        int count = 0;
        long preInitializeEpoch = -1;
        for (UntypedResultSet.Row row : executeInternal(query))
        {
            count++;
            if (Transformation.Kind.fromId(row.getInt("kind")) == Transformation.Kind.PRE_INITIALIZE_CMS)
                preInitializeEpoch = row.getLong("epoch");
        }
        if (count == 1 && preInitializeEpoch != -1)
        {
            logger.warn("Cleaning up orphaned PreInitialize at epoch {} - restarting in gossip mode", preInitializeEpoch);
            String cleanupQuery = String.format("DELETE from %s.%s where epoch = %d", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME, preInitializeEpoch);
            executeInternal(cleanupQuery);
            return false;
        }

        return count > 0;
    }

    @Override
    public MetadataSnapshots snapshots()
    {
        return snapshots.get();
    }

    @VisibleForTesting
    public void invalidateTail()
    {
        clearTail();
    }

    public void truncate()
    {
        invalidateTail();
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(NAME).truncateBlockingWithoutSnapshot();
    }

    /**
     * Gets the persisted log state for replaying log on startup
     *
     * Slow, only to be used on startup
     *
     * @return
     */
    @Override
    public LogState getPersistedLogState()
    {
        ClusterMetadata base = snapshots.get().getLatestSnapshot();
        return getLogStateBetween(base, Epoch.create(Long.MAX_VALUE));
    }

    @Override
    public EntryHolder getEntries(Epoch since) throws IOException
    {
        // during gossip upgrade we have epoch = Long.MIN_VALUE + 1 (and the reverse partitioner doesn't support negative keys)
        since = since.isBefore(Epoch.EMPTY) ? Epoch.EMPTY : since;
        EntryHolder fromTail = fromTail(since);
        if (fromTail != null)
            return fromTail;
        UntypedResultSet resultSet = executeInternal(String.format("SELECT epoch, kind, transformation, entry_id FROM %s.%s WHERE token(epoch) <= token(?)", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME),
                                                     since.getEpoch());
        return toEntryHolder(since, resultSet);
    }

    /**
     * Returns the entries after {@code since} if the tail holds all of them, else null. The tail can change while it
     * is read (trimmed at the front, or cleared at a gap), so check the count rather than trust the range.
     */
    private EntryHolder fromTail(Epoch since)
    {
        Map.Entry<Long, WeightedEntry> first = tail.firstEntry();
        Map.Entry<Long, WeightedEntry> last = tail.lastEntry();
        if (first == null || last == null || since.getEpoch() >= last.getKey() || first.getKey() > since.getEpoch() + 1)
            return null;

        EntryHolder holder = new EntryHolder(since);
        for (WeightedEntry weighted : tail.subMap(since.getEpoch() + 1, true, last.getKey(), true).values())
            holder.add(weighted.entry);
        return holder.entries.size() == last.getKey() - since.getEpoch() ? holder : null;
    }

    public EntryHolder getEntries(Epoch since, Epoch until) throws IOException
    {
        // during gossip upgrade we have epoch = Long.MIN_VALUE + 1 (and the reverse partitioner doesn't support negative keys)
        since = since.isBefore(Epoch.EMPTY) ? Epoch.EMPTY : since;
        UntypedResultSet resultSet = executeInternal(String.format("SELECT epoch, kind, transformation, entry_id " +
                                                                   "FROM %s.%s " +
                                                                   "WHERE token(epoch) <= token(?) AND token(epoch) >= token(?)",
                                                                   SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME),
                                                     since.getEpoch(), until.getEpoch());
        return toEntryHolder(since, resultSet);
    }

    private static EntryHolder toEntryHolder(Epoch since, UntypedResultSet resultSet) throws IOException
    {
        EntryHolder holder = new EntryHolder(since);
        for (UntypedResultSet.Row row : resultSet)
        {
            long entryId = row.getLong("entry_id");
            Epoch epoch = Epoch.create(row.getLong("epoch"));
            Transformation.Kind kind = Transformation.Kind.fromId(row.getInt("kind"));
            Transformation transform = kind.fromVersionedBytes(row.getBlob("transformation"));
            holder.add(new Entry(new Entry.Id(entryId), epoch, transform));
        }
        return holder;
    }

    @Override
    public LogState getLogStateBetween(ClusterMetadata base, Epoch end)
    {
        try
        {
            Epoch epoch = base == null ? Epoch.EMPTY : base.epoch;
            EntryHolder entryHolder = getEntries(epoch, end);
            ImmutableList.Builder<Entry> entries = ImmutableList.builder();
            Epoch prevEpoch = epoch;
            for (Entry e : entryHolder.entries)
            {
                if (!prevEpoch.nextEpoch().is(e.epoch))
                    throw new IllegalStateException("Can't get replication between " + epoch + " and " + end + " - incomplete local log?");
                prevEpoch = e.epoch;
                entries.add(e);
            }
            return new LogState(base, entries.build());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}