/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. The ASF licenses this
 * file to you under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.cassandra.db.compaction.unified;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ControllerHierarchyTest extends CQLTester
{
    @Test
    public void realFlushEstimatesPromoteLargeFilesAcrossLevelGaps() throws Throwable
    {
        ColumnFamilyStore cfs = createSmallHierarchyTable();
        Controller empty = Controller.fromOptions(cfs, Map.of("min_hierarchy_size", "1KiB"));
        assertEquals(1024 * 0.775, empty.getBaseSstableSize(4), 0.0);
        execute("INSERT INTO %s (pk, c, v) VALUES (0, 0, ?)", ByteBuffer.wrap(new byte[256]));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        Random random = new Random(7529);
        for (int i = 1; i <= 512; i++)
        {
            byte[] bytes = new byte[4096];
            random.nextBytes(bytes);
            execute("INSERT INTO %s (pk, c, v) VALUES (0, ?, ?)", i, ByteBuffer.wrap(bytes));
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("INSERT INTO %s (pk, c, v) VALUES (0, 513, ?)", ByteBuffer.wrap(new byte[256]));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        SSTableReader first = cfs.getLiveSSTables().iterator().next();
        UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(first);
        List<UnifiedCompactionStrategy.Level> levels = ucs.getLevels(cfs.getLiveSSTables(), s -> true);
        assertTrue(levels.size() >= 3);
        assertEquals(2, levels.get(0).getSSTables().size());
        assertTrue(levels.get(1).getSSTables().isEmpty());
        assertEquals(1, levels.get(levels.size() - 1).getSSTables().size());
        assertTrue(ucs.getController().getFlushSizeBytes() < 1 << 20);
        assertTrue(cfs.getLiveSSTables().stream().anyMatch(s -> s.onDiskLength() > 1 << 20));
        Set<SSTableReader> assigned = new HashSet<>();
        levels.forEach(level -> level.getSSTables().forEach(s -> assertTrue(assigned.add(s))));
        assertEquals(cfs.getLiveSSTables().size(), assigned.size());
        assertRows(execute("SELECT count(*) FROM %s"), row(514L));
        assertFalse(((TrieMemtable) cfs.getCurrentMemtable()).isInitialized());
    }

    @Test
    public void compactionPreservesDeletesAndTtlAtExplicitReadTimes() throws Throwable
    {
        ColumnFamilyStore cfs = createSmallHierarchyTable();
        for (int pk = 0; pk < 4; pk++)
            for (int c = 0; c < 4; c++)
                execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", pk, c, ByteBuffer.wrap(new byte[] { 1 }));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("DELETE FROM %s WHERE pk = 0");
        execute("DELETE FROM %s WHERE pk = 1 AND c = 0");
        execute("DELETE FROM %s WHERE pk = 2 AND c >= 1 AND c <= 2");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        long now = FBUtilities.nowInSeconds() + 3600;
        QueryProcessor.executeInternalWithNowInSec(formatQuery("INSERT INTO %s (pk, c, v) VALUES (9, 0, 0x01) USING TTL 60"), now);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        boolean cursor = DatabaseDescriptor.cursorCompactionEnabled();
        try
        {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            for (int pass = 0; pass < 2; pass++)
            {
                assertEquals(10L, QueryProcessor.executeInternalWithNowInSec(formatQuery("SELECT count(*) FROM %s"), now + 59).one().getLong("count"));
                assertEquals(9L, QueryProcessor.executeInternalWithNowInSec(formatQuery("SELECT count(*) FROM %s"), now + 61).one().getLong("count"));
                assertEmpty(execute("SELECT * FROM %s WHERE pk = 0"));
                assertEmpty(execute("SELECT * FROM %s WHERE pk = 1 AND c = 0"));
                assertEmpty(execute("SELECT * FROM %s WHERE pk = 2 AND c >= 1 AND c <= 2"));
                if (pass == 0)
                {
                    SSTableReader first = cfs.getLiveSSTables().iterator().next();
                    UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) cfs.getCompactionStrategyManager().getCompactionStrategyFor(first);
                    AtomicBoolean readDuringCompaction = new AtomicBoolean();
                    ucs.getUserDefinedTask(cfs.getLiveSSTables(), now - 86400).execute(new ActiveCompactionsTracker()
                    {
                        public void beginCompaction(CompactionInfo.Holder info)
                        {
                            assertFalse(cfs.getTracker().getCompacting().isEmpty());
                            assertEquals(10L, QueryProcessor.executeInternalWithNowInSec(formatQuery("SELECT count(*) FROM %s"), now + 59).one().getLong("count"));
                            assertEquals(9L, QueryProcessor.executeInternalWithNowInSec(formatQuery("SELECT count(*) FROM %s"), now + 61).one().getLong("count"));
                            readDuringCompaction.set(true);
                        }

                        public void finishCompaction(CompactionInfo.Holder info)
                        {
                        }
                    });
                    assertTrue(readDuringCompaction.get());
                }
            }
            assertEquals(1, cfs.getLiveSSTables().size());
            execute("INSERT INTO %s (pk, c, v) VALUES (10, 0, 0x02)");
            assertTrue(((TrieMemtable) cfs.getCurrentMemtable()).isInitialized());
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            assertRows(execute("SELECT v FROM %s WHERE pk = 10"), row(ByteBuffer.wrap(new byte[] { 2 })));
        }
        finally
        {
            DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        }
    }

    private ColumnFamilyStore createSmallHierarchyTable()
    {
        createTable("CREATE TABLE %s (pk int, c int, v blob, PRIMARY KEY (pk,c)) WITH memtable = 'trie'" +
                    " AND compression = {'enabled':'false'} AND compaction = {'class':'UnifiedCompactionStrategy'," +
                    "'min_hierarchy_size':'1KiB','min_sstable_size':'100MiB'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }
}
