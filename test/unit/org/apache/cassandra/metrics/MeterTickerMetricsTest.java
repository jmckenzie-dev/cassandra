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
package org.apache.cassandra.metrics;

import java.util.List;

import com.codahale.metrics.Gauge;

import org.junit.Test;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MeterTickerMetricsTest
{
    /**
     * Registration runs in CassandraDaemon.setup. The registry rejects types missing from its metricGroups
     * set, and that rejection once stopped every node at startup; this test fails first.
     */
    @Test
    public void registersGaugesInAKnownGroup()
    {
        assertTrue(CassandraMetricsRegistry.metricGroups.contains(MeterTickerMetrics.TYPE_NAME));
        MeterTickerMetrics.register();
        MeterTickerMetrics.register(); // a second call is a no-op

        for (String name : List.of("RegisteredMeters", "ClearedReferencesRemoved", "TickFailures", "LastTickPassMicros"))
        {
            Gauge<?> gauge = Metrics.getGauges().get("org.apache.cassandra.metrics.MeterTicker." + name);
            assertNotNull(name, gauge);
            assertNotNull(name, gauge.getValue());
        }
    }
}
