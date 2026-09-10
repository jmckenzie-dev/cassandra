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

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;

/**
 * Checkpoint lifecycle for one read command. A local stop is not an accepted coordinator continuation.
 * Local execution and coordinator acceptance use independent command state until response acceptance.
 * Payload resets retain pager ownership; command copies inherit ownership, never the payload or this object.
 * Callers retain the existing execution and response-publication ordering; this class adds no synchronization.
 */
final class TombstonePagingState
{
    private boolean pagerOwned;
    private Row checkpointRow;
    private int tombstoneCount;

    void attachPager()
    {
        pagerOwned = true;
    }

    boolean hasPager()
    {
        return pagerOwned;
    }

    /** Initialize a new command's already allocated state without sharing mutable execution state. */
    void initializeForNewExecution(TombstonePagingState source)
    {
        pagerOwned = source.pagerOwned;
        clearCheckpoint();
    }

    void beginLocalExecution()
    {
        clearCheckpoint();
    }

    /** Local stops can occur on commands without a pager; acceptance is a separate decision. */
    void recordLocalStop(Row row, int count)
    {
        checkpointRow = row;
        tombstoneCount = count;
    }

    void acceptContinuation(@Nullable TombstonePageContinuation continuation, int count)
    {
        Preconditions.checkState(pagerOwned);
        checkpointRow = continuation == null ? null : BTreeRow.emptyRow(continuation.clustering);
        tombstoneCount = continuation == null ? 0 : count;
    }

    @Nullable
    Row consumeForReconciliation()
    {
        Row row = checkpointRow;
        clearCheckpoint();
        return row;
    }

    @Nullable
    Row checkpointRow()
    {
        return checkpointRow;
    }

    int tombstoneCount()
    {
        return tombstoneCount;
    }

    private void clearCheckpoint()
    {
        checkpointRow = null;
        tombstoneCount = 0;
    }
}
