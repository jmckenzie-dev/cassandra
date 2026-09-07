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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional.State.IN_PROGRESS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TombstoneCompactionShutdownTest extends TestBaseImpl
{
    private static final String TABLE = "shutdown_test";

    @Test
    public void drainInterruptsActiveAndDiscardsQueuedWork() throws Exception
    {
        try (Cluster cluster = createCluster())
        {
            prepare(cluster);
            cluster.get(1).runOnInstance(rethrow(() -> {
                ColumnFamilyStore cfs = table();
                Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
                Gate gate = GatedStrategy.gate;
                FutureTask<Void> shutdown = null;
                try
                {
                    startActiveAndQueued(cfs, gate);
                    shutdown = new FutureTask<>(() -> {
                        StorageService.instance.drain();
                        return null;
                    });
                    executorFactory().startThread("test-drain", shutdown);
                    assertTrue("Drain did not interrupt reactive compaction", gate.interrupted.await(30, SECONDS));
                    submit(cfs, 2);
                    assertWaiting(shutdown);
                    gate.release.countDown();
                    shutdown.get(30, SECONDS);

                    assertStopped(cfs, gate);
                    assertEquals(singleton(0), gate.scanned);
                    assertEquals("Cancellation must preserve the original SSTables", originals, cfs.getLiveSSTables());
                }
                finally
                {
                    gate.release.countDown();
                    if (shutdown != null)
                        shutdown.get(30, SECONDS);
                }
            }));
            cluster.get(1).shutdown().get(60, SECONDS);
            cluster.get(1).startup();
            assertData(cluster);
        }
    }

    @Test
    public void gracefulShutdownWaitsForActiveAndQueuedWork() throws Exception
    {
        try (Cluster cluster = createCluster())
        {
            prepare(cluster);
            cluster.get(1).runOnInstance(rethrow(() -> {
                ColumnFamilyStore cfs = table();
                SSTableReader first = sstable(cfs, 0);
                SSTableReader second = sstable(cfs, 1);
                SSTableReader late = sstable(cfs, 2);
                Gate gate = GatedStrategy.gate;
                FutureTask<Void> shutdown = null;
                try
                {
                    startActiveAndQueued(cfs, gate);
                    shutdown = new FutureTask<>(() -> {
                        CompactionManager.instance.finishCompactionsAndShutdown(30, SECONDS);
                        return null;
                    });
                    executorFactory().startThread("test-graceful-compaction-shutdown", shutdown);
                    // The regular executor closes after reactive admission closes in the production helper.
                    await().atMost(10, SECONDS).until(() -> CompactionManager.instance.submitUserDefined(cfs, emptyList(), 0).isCancelled());
                    submit(cfs, 2);
                    assertWaiting(shutdown);
                    assertTrue(cfs.getTracker().getCompacting().contains(first));
                    gate.release.countDown();
                    shutdown.get(30, SECONDS);

                    assertStopped(cfs, gate);
                    assertEquals(Set.of(0, 1), gate.scanned);
                    assertEquals("Graceful shutdown must not interrupt the worker", 1, gate.interrupted.getCount());
                    assertFalse(cfs.getLiveSSTables().contains(first));
                    assertFalse(cfs.getLiveSSTables().contains(second));
                    assertTrue("Work submitted during shutdown must not run", cfs.getLiveSSTables().contains(late));
                }
                finally
                {
                    gate.release.countDown();
                    if (shutdown != null)
                        shutdown.get(30, SECONDS);
                }
            }));
            assertData(cluster);
        }
    }

    @Test
    public void gracefulShutdownDropsBusyRetryWithoutClosingItsOwner() throws Exception
    {
        try (Cluster cluster = createCluster())
        {
            prepare(cluster);
            cluster.get(1).runOnInstance(rethrow(() -> {
                ColumnFamilyStore cfs = table();
                Set<SSTableReader> originals = new HashSet<>(cfs.getLiveSSTables());
                SSTableReader busy = sstable(cfs, 0);
                Gate gate = GatedStrategy.gate;
                try (LifecycleTransaction owner = cfs.getTracker().tryModify(busy, OperationType.COMPACTION))
                {
                    assertNotNull(owner);
                    submit(cfs, 0);
                    await().atMost(10, SECONDS).until(() -> gate.busyAttempts.get() >= 2);
                    CompactionManager.instance.finishCompactionsAndShutdown(10, SECONDS);

                    assertFalse(CompactionManager.instance.hasOngoingOrPendingTasks());
                    await().atMost(10, SECONDS).until(() -> !gate.worker.isAlive());
                    assertEquals(IN_PROGRESS, owner.state());
                    assertEquals(singleton(busy), cfs.getTracker().getCompacting());
                    assertEquals(originals, cfs.getLiveSSTables());
                    assertTrue(gate.scanned.isEmpty());
                    submit(cfs, 2);
                    assertFalse("Shutdown must reject new reactive work", CompactionManager.instance.hasOngoingOrPendingTasks());
                }
                finally
                {
                    gate.release.countDown();
                }
                assertTrue(cfs.getTracker().getCompacting().isEmpty());
            }));
            assertData(cluster);
        }
    }

    private Cluster createCluster() throws Exception
    {
        return init(builder().withNodes(1)
                             .withConfig(config -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                         .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                                         .set("tombstone_compaction_queue_capacity", 4))
                             .start());
    }

    private static void prepare(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (id int PRIMARY KEY, value int, deleted int) " +
                             "WITH compaction = {'class': '" + GatedStrategy.class.getName() + "', 'enabled': 'false'}");
        for (int key = 0; key < 3; key++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, value, deleted) VALUES (?, ?, 1)",
                                           ConsistencyLevel.ALL, key, key);
            cluster.coordinator(1).execute("DELETE deleted FROM " + KEYSPACE + '.' + TABLE + " WHERE id = ?",
                                           ConsistencyLevel.ALL, key);
            cluster.get(1).flush(KEYSPACE);
        }
        cluster.get(1).runOnInstance(() -> {
            assertEquals(3, table().getLiveSSTables().size());
            GatedStrategy.gate = new Gate();
        });
    }

    private static void assertData(Cluster cluster)
    {
        for (int key = 0; key < 3; key++)
            assertRows(cluster.coordinator(1).execute("SELECT id, value, deleted FROM " + KEYSPACE + '.' + TABLE + " WHERE id = ?",
                                                      ConsistencyLevel.ALL, key), row(key, key, null));
    }

    private static ColumnFamilyStore table()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    private static SSTableReader sstable(ColumnFamilyStore cfs, int key)
    {
        return cfs.getLiveSSTables().stream().filter(s -> ByteBufferUtil.toInt(s.getFirst().getKey()) == key).findFirst().orElseThrow();
    }

    private static void submit(ColumnFamilyStore cfs, int key)
    {
        CompactionManager.instance.submitTombstoneTriggeredCompaction(cfs, cfs.decorateKey(ByteBufferUtil.bytes(key)), 1);
    }

    private static void startActiveAndQueued(ColumnFamilyStore cfs, Gate gate) throws InterruptedException
    {
        submit(cfs, 0);
        assertTrue("Reactive compaction did not reach the scanner", gate.entered.await(30, SECONDS));
        assertEquals(singleton(sstable(cfs, 0)), cfs.getTracker().getCompacting());
        assertTrue(gate.worker.getName(), gate.worker.getName().contains("TombstoneCompactionExecutor"));
        submit(cfs, 1);
    }

    private static void assertStopped(ColumnFamilyStore cfs, Gate gate)
    {
        assertFalse("Shutdown returned with reactive work outstanding", CompactionManager.instance.hasOngoingOrPendingTasks());
        assertTrue("Compaction leaked its SSTable reservation", cfs.getTracker().getCompacting().isEmpty());
        await().atMost(10, SECONDS).until(() -> !gate.worker.isAlive());
    }

    private static void assertWaiting(FutureTask<Void> shutdown) throws Exception
    {
        try
        {
            shutdown.get(200, MILLISECONDS);
        }
        catch (TimeoutException expected)
        {
            return;
        }
        fail("Shutdown returned while reactive compaction was held");
    }

    private static class Gate
    {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Set<Integer> scanned = ConcurrentHashMap.newKeySet();
        final AtomicInteger busyAttempts = new AtomicInteger();
        volatile Thread worker;
    }

    /** Adds a gate to real SSTable scanners through the table's configured strategy. */
    public static class GatedStrategy extends SizeTieredCompactionStrategy
    {
        private static volatile Gate gate;

        public GatedStrategy(ColumnFamilyStore cfs, Map<String, String> options)
        {
            super(cfs, options);
        }

        @Override
        public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, long gcBefore)
        {
            AbstractCompactionTask task = super.getUserDefinedTask(sstables, gcBefore);
            if (gate != null && task == null)
            {
                gate.worker = Thread.currentThread();
                gate.busyAttempts.incrementAndGet();
            }
            return task;
        }

        @Override
        public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            ScannerList scanners = super.getScanners(sstables, ranges);
            if (gate != null)
                scanners.scanners.replaceAll(scanner -> new GatedScanner(scanner, gate));
            return scanners;
        }
    }

    private static class GatedScanner implements ISSTableScanner
    {
        private final ISSTableScanner delegate;
        private final Gate gate;
        private boolean entered;

        private GatedScanner(ISSTableScanner delegate, Gate gate)
        {
            this.delegate = delegate;
            this.gate = gate;
        }

        @Override
        public boolean hasNext()
        {
            if (!entered)
            {
                entered = true;
                gate.worker = Thread.currentThread();
                int key = ByteBufferUtil.toInt(delegate.getBackingSSTables().iterator().next().getFirst().getKey());
                gate.scanned.add(key);
                if (key == 0)
                {
                    gate.entered.countDown();
                    try
                    {
                        assertTrue("Timed out waiting to release reactive compaction", gate.release.await(60, SECONDS));
                    }
                    catch (InterruptedException e)
                    {
                        gate.interrupted.countDown();
                        // Keep the reservation until the test has checked admission during drain.
                        assertTrue("Timed out releasing interrupted compaction", awaitUninterruptibly(gate.release, 30, SECONDS));
                        throw new UncheckedInterruptedException(e);
                    }
                }
            }
            return delegate.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return delegate.next();
        }

        @Override
        public TableMetadata metadata()
        {
            return delegate.metadata();
        }

        @Override
        public long getLengthInBytes()
        {
            return delegate.getLengthInBytes();
        }

        @Override
        public long getCompressedLengthInBytes()
        {
            return delegate.getCompressedLengthInBytes();
        }

        @Override
        public long getCurrentPosition()
        {
            return delegate.getCurrentPosition();
        }

        @Override
        public long getBytesScanned()
        {
            return delegate.getBytesScanned();
        }

        @Override
        public Set<SSTableReader> getBackingSSTables()
        {
            return delegate.getBackingSSTables();
        }

        @Override
        public boolean isFullRange()
        {
            return delegate.isFullRange();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
