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
package org.apache.cassandra.utils.streamhist;

import java.lang.management.ManagementFactory;

import com.sun.management.ThreadMXBean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.io.sstable.SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE;
import static org.apache.cassandra.io.sstable.SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE;
import static org.apache.cassandra.io.sstable.SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TombstoneHistogramAllocationTest
{
    private static final Logger logger = LoggerFactory.getLogger(TombstoneHistogramAllocationTest.class);
    private static final int BATCH_SIZE = 8;
    private static final int SAMPLES = 3;

    private static volatile TombstoneHistogramBuilder builderSink;
    private static volatile TombstoneHistogram histogramSink;

    @Test
    public void emptyLifecycleAvoidsSpoolAllocationAndFirstObservationPaysForIt()
    {
        java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        assumeTrue("Thread allocation counters require the HotSpot management extension", platformBean instanceof ThreadMXBean);
        ThreadMXBean bean = (ThreadMXBean) platformBean;
        assumeTrue("Thread allocation counters are unavailable", bean.isThreadAllocatedMemorySupported());
        boolean wasEnabled = bean.isThreadAllocatedMemoryEnabled();
        try
        {
            if (!wasEnabled)
                bean.setThreadAllocatedMemoryEnabled(true);

            for (boolean populated : new boolean[] { false, true })
                for (boolean lazy : new boolean[] { false, true })
                    measure(bean, lazy, populated, 4);

            logger.info("Histogram allocation probe: bins={} spool={} rounding={} batch={} samples={}",
                              TOMBSTONE_HISTOGRAM_BIN_SIZE, TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                              TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS, BATCH_SIZE, SAMPLES);
            for (int sample = 0; sample < SAMPLES; sample++)
            {
                long eagerEmpty;
                long lazyEmpty;
                long eagerPopulated;
                long lazyPopulated;
                if ((sample & 1) == 0)
                {
                    eagerEmpty = measure(bean, false, false, BATCH_SIZE);
                    lazyEmpty = measure(bean, true, false, BATCH_SIZE);
                    eagerPopulated = measure(bean, false, true, BATCH_SIZE);
                    lazyPopulated = measure(bean, true, true, BATCH_SIZE);
                }
                else
                {
                    lazyPopulated = measure(bean, true, true, BATCH_SIZE);
                    eagerPopulated = measure(bean, false, true, BATCH_SIZE);
                    lazyEmpty = measure(bean, true, false, BATCH_SIZE);
                    eagerEmpty = measure(bean, false, false, BATCH_SIZE);
                }

                logger.info("Histogram allocation sample={} batch_bytes eager_empty={} lazy_empty={} eager_populated={} lazy_populated={}",
                                  sample, eagerEmpty, lazyEmpty, eagerPopulated, lazyPopulated);
                logger.info("Histogram allocation sample={} bytes_per_lifecycle eager_empty={} lazy_empty={} eager_populated={} lazy_populated={}",
                                  sample, (double) eagerEmpty / BATCH_SIZE, (double) lazyEmpty / BATCH_SIZE,
                                  (double) eagerPopulated / BATCH_SIZE, (double) lazyPopulated / BATCH_SIZE);

                assertTrue("Empty lazy lifecycle must avoid at least 99% of reference allocation", lazyEmpty * 100 < eagerEmpty);
                assertTrue("First observation must allocate the deferred spool", lazyPopulated > lazyEmpty * 100);
                assertTrue("Populated lifecycle allocation must remain within 1% of the reference",
                           Math.abs(lazyPopulated - eagerPopulated) * 100 < eagerPopulated);
            }
        }
        finally
        {
            builderSink = null;
            histogramSink = null;
            if (!wasEnabled)
                bean.setThreadAllocatedMemoryEnabled(false);
        }
    }

    private static long measure(ThreadMXBean bean, boolean lazy, boolean populated, int operations)
    {
        long threadId = Thread.currentThread().getId();
        int observedBins = 0;
        long before = bean.getThreadAllocatedBytes(threadId);
        for (int operation = 0; operation < operations; operation++)
        {
            TombstoneHistogramBuilder builder = lazy
                                                ? new LazyTombstoneHistogramBuilder(TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                   TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                   TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS)
                                                : new StreamingTombstoneHistogramBuilder(TOMBSTONE_HISTOGRAM_BIN_SIZE,
                                                                                        TOMBSTONE_HISTOGRAM_SPOOL_SIZE,
                                                                                        TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
            builderSink = builder;
            if (populated)
                builder.update(3600L);
            TombstoneHistogram histogram = builder.build();
            histogramSink = histogram;
            observedBins += histogram.size();
            builder.releaseBuffers();
        }
        long after = bean.getThreadAllocatedBytes(threadId);
        assertTrue("Thread allocation counters must remain available", before >= 0 && after >= before);
        assertEquals(populated ? operations : 0, observedBins);
        return after - before;
    }
}
