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
package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TrieMemtableLazyPropertyTest extends CQLTester
{
    @Test
    public void generatedOperationsPreserveRowsAcrossActivationAndFlush() throws Throwable
    {
        for (long seed : new long[] { 1L, 17L, 932451L, 0x5eedL })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            Map<Integer, Map<Integer, Integer>> model = new HashMap<>();
            Random random = new Random(seed);
            List<String> trace = new ArrayList<>();
            for (int step = 0; step < 64; step++)
            {
                int operation = random.nextInt(10);
                int pk = random.nextInt(8);
                int ck = random.nextInt(3);
                int value = random.nextInt();
                long timestamp = step + 1L;
                trace.add(operation + ":" + pk + ":" + ck + ":" + value);
                try
                {
                    switch (operation)
                    {
                        case 0:
                        case 1:
                        case 2:
                            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?", pk, ck, value, timestamp);
                            model.computeIfAbsent(pk, ignored -> new HashMap<>()).put(ck, value);
                            assertTrue(current(cfs).isInitialized());
                            break;
                        case 3:
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ? AND ck = ?", timestamp, pk, ck);
                            Map<Integer, Integer> partition = model.get(pk);
                            if (partition != null)
                                partition.remove(ck);
                            assertTrue(current(cfs).isInitialized());
                            break;
                        case 4:
                            execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = ?", timestamp, pk);
                            model.remove(pk);
                            assertTrue(current(cfs).isInitialized());
                            break;
                        case 5:
                            cfs.switchMemtable(ColumnFamilyStore.FlushReason.UNIT_TESTS).get(30, TimeUnit.SECONDS);
                            assertFalse(current(cfs).isInitialized());
                            break;
                        case 6:
                            execute("TRUNCATE %s");
                            model.clear();
                            assertFalse(current(cfs).isInitialized());
                            break;
                        case 7:
                            break;
                        case 8:
                            retire(cfs);
                            break;
                        case 9:
                            retire(cfs);
                            retire(cfs);
                            break;
                        default:
                            throw new AssertionError(operation);
                    }
                    boolean initialized = current(cfs).isInitialized();
                    assertModel(model);
                    if (!initialized)
                        assertFalse("Reads initialized the memtable", current(cfs).isInitialized());
                }
                catch (Throwable failure)
                {
                    throw new AssertionError("seed=" + seed + ", step=" + step + ", operations=" + trace, failure);
                }
            }
            retire(cfs);
            retire(cfs);
            assertModel(model);
            assertFalse(current(cfs).isInitialized());
        }
    }

    private static void retire(ColumnFamilyStore cfs) throws Exception
    {
        TrieMemtable previous = current(cfs);
        boolean clean = previous.isClean();
        int sstables = cfs.getLiveSSTables().size();
        cfs.forceFlush(ColumnFamilyStore.FlushReason.USER_FORCED).get(30, TimeUnit.SECONDS);
        assertFalse(current(cfs).isInitialized());
        assertTrue(current(cfs).isClean());
        if (clean)
        {
            assertSame(previous, current(cfs));
            assertEquals(sstables, cfs.getLiveSSTables().size());
        }
        else
        {
            assertNotSame(previous, current(cfs));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((previous.getAllocator().onHeap().owns() != 0 || previous.getAllocator().offHeap().owns() != 0)
                   && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertEquals("Retired on-heap accounting", 0L, previous.getAllocator().onHeap().owns());
            assertEquals("Retired off-heap accounting", 0L, previous.getAllocator().offHeap().owns());
        }
    }

    private void assertModel(Map<Integer, Map<Integer, Integer>> model) throws Throwable
    {
        List<Object[]> expected = new ArrayList<>();
        for (int pk = 0; pk < 8; pk++)
        {
            List<Object[]> partitionRows = new ArrayList<>();
            Map<Integer, Integer> partition = model.get(pk);
            for (int ck = 0; ck < 3; ck++)
            {
                if (partition != null && partition.containsKey(ck))
                {
                    Object[] row = row(pk, ck, partition.get(ck));
                    partitionRows.add(row);
                    expected.add(row);
                }
            }
            assertRows(execute("SELECT pk, ck, v FROM %s WHERE pk = ?", pk), partitionRows.toArray(new Object[0][]));
        }
        assertRowsIgnoringOrder(execute("SELECT pk, ck, v FROM %s"), expected.toArray(new Object[0][]));
    }

    private static TrieMemtable current(ColumnFamilyStore cfs)
    {
        return (TrieMemtable) cfs.getCurrentMemtable();
    }
}
