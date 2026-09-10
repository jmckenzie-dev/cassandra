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
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TombstonePagingStateTest
{
    private static final Row ROW = BTreeRow.emptyRow(Clustering.make(ByteBufferUtil.bytes(7)));

    @Test
    public void localStopsDoNotRequirePagerOwnership()
    {
        TombstonePagingState state = new TombstonePagingState();
        assertEmpty(state);
        assertFalse(state.hasPager());
        state.recordLocalStop(ROW, 3);
        assertSame(ROW, state.checkpointRow());
        assertEquals(3, state.tombstoneCount());
        assertFalse(state.hasPager());
        state.beginLocalExecution();
        assertEmpty(state);
        assertFalse(state.hasPager());
    }

    @Test
    public void rejectedPublicationDoesNotChangeLocalStop()
    {
        TombstonePagingState state = new TombstonePagingState();
        state.recordLocalStop(ROW, 3);
        assertThatThrownBy(() -> state.acceptContinuation(new TombstonePageContinuation(ROW.clustering()), 8))
        .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> state.acceptContinuation(null, 0)).isInstanceOf(IllegalStateException.class);
        assertSame(ROW, state.checkpointRow());
        assertEquals(3, state.tombstoneCount());
    }

    @Test
    public void acceptanceAndResetsRetainPagerOwnership()
    {
        TombstonePagingState state = new TombstonePagingState();
        state.attachPager();
        state.acceptContinuation(new TombstonePageContinuation(ROW.clustering()), 5);
        assertEquals(ROW.clustering(), state.checkpointRow().clustering());
        assertEquals(5, state.tombstoneCount());
        state.beginLocalExecution();
        assertEmpty(state);
        assertTrue(state.hasPager());
        state.recordLocalStop(ROW, 3);
        assertSame(ROW, state.consumeForReconciliation());
        assertEmpty(state);
        assertTrue(state.hasPager());
        assertNull(state.consumeForReconciliation());
        state.acceptContinuation(new TombstonePageContinuation(ROW.clustering()), 6);
        state.acceptContinuation(null, 9);
        assertEmpty(state);
        assertTrue(state.hasPager());
    }

    @Test
    public void newExecutionInheritsOnlyOwnership()
    {
        for (boolean owned : new boolean[]{ false, true })
        {
            TombstonePagingState source = new TombstonePagingState();
            if (owned)
                source.attachPager();
            source.recordLocalStop(ROW, 3);
            TombstonePagingState copy = new TombstonePagingState();
            copy.initializeForNewExecution(source);
            assertEquals(owned, copy.hasPager());
            assertEmpty(copy);
            copy.recordLocalStop(BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING), 1);
            assertSame(ROW, source.checkpointRow());
            assertEquals(3, source.tombstoneCount());
            source.consumeForReconciliation();
            assertSame(Clustering.STATIC_CLUSTERING, copy.checkpointRow().clustering());
            assertEquals(1, copy.tombstoneCount());
            assertEquals(owned, source.hasPager());
        }
    }

    private static void assertEmpty(TombstonePagingState state)
    {
        assertNull(state.checkpointRow());
        assertEquals(0, state.tombstoneCount());
    }
}
