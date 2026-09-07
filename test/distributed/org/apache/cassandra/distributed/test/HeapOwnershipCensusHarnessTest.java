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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HeapOwnershipCensusHarnessTest
{
    @Test
    public void boundsAllTableCountsAroundTheSupportedRange()
    {
        for (int tables = -10; tables <= 1010; tables++)
        {
            String[] args = { "--tables", Integer.toString(tables) };
            if (tables >= 1 && tables <= 1000)
                assertEquals(tables, HeapOwnershipCensusHarness.Config.parse(args).tables);
            else
                assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void validatesSubnetRangeAndArguments()
    {
        for (int subnet = -10; subnet <= 270; subnet++)
        {
            String[] args = { "--subnet", Integer.toString(subnet) };
            if (subnet >= 0 && subnet <= 255)
                assertEquals(subnet, HeapOwnershipCensusHarness.Config.parse(args).subnet);
            else
                assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(args)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--tables" })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--unknown", "x" })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HeapOwnershipCensusHarness.Config.parse(new String[]{ "--tables", "x" })).isInstanceOf(IllegalArgumentException.class);
        HeapOwnershipCensusHarness.Config defaults = HeapOwnershipCensusHarness.Config.parse(new String[0]);
        assertEquals(100, defaults.tables);
        assertTrue(defaults.heapDumps);
        assertTrue(defaults.inspectNameProperties);
        assertFalse(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--no-heap-dumps" }).heapDumps);
        assertFalse(HeapOwnershipCensusHarness.Config.parse(new String[]{ "--attributes-only" }).inspectNameProperties);
    }

    @Test
    public void recognizesUserKeyspaceWithoutParsingObjectNameProperties()
    {
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census,name=ReadLatency,type=Table"));
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:foo=x,keyspace=heap_census,name=ReadLatency"));
        assertTrue(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=heap_census_extra,name=ReadLatency"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:otherkeyspace=heap_census,name=ReadLatency"));
        assertFalse(HeapOwnershipCensusHarness.hasUserKeyspace("org.apache.cassandra.metrics:keyspace=system,name=ReadLatency"));
    }
}
