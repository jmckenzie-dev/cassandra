/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TableMetricBookkeepingLifecycleTest extends CQLTester
{
    @Test
    public void subclassesCanAddAndFindMetricsAfterConstructionAndReleaseTwice() throws Throwable
    {
        for (boolean compact : new boolean[] { false, true })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.metric.release();
            LateMetrics metrics = new LateMetrics(cfs, compact);
            String name = "LateBookkeeping" + compact;
            MetricNameFactory factory = new TableMetrics.TableMetricNameFactory(cfs, TableMetrics.TYPE_NAME);
            MetricNameFactory global = new TableMetrics.AllTableMetricNameFactory(TableMetrics.TYPE_NAME);
            try
            {
                Gauge<Long> first = metrics.add(name, 7);
                for (int i = 0; i < 200; i++)
                    metrics.add(name + i, i);
                assertSame(first, metrics.add(name, 99));
                assertEquals(7L, first.getValue().longValue());
                assertEquals(7L, ((Number) Metrics.getGauges().get(global.createMetricName(name).getMetricName()).getValue()).longValue());
                assertTrue(Metrics.getMetrics().containsKey(factory.createMetricName(name).getMetricName()));
            }
            finally
            {
                metrics.release();
                metrics.release();
            }
            assertFalse(Metrics.getMetrics().containsKey(factory.createMetricName(name).getMetricName()));
            assertEquals(0L, ((Number) Metrics.getGauges().get(global.createMetricName(name).getMetricName()).getValue()).longValue());
        }
    }

    private static final class LateMetrics extends TableMetrics
    {
        private LateMetrics(ColumnFamilyStore cfs, boolean compact)
        {
            super(cfs, compact);
        }

        private Gauge<Long> add(String name, long value)
        {
            return createTableGauge(name, () -> value);
        }
    }
}
