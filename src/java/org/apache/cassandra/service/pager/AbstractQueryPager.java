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
package org.apache.cassandra.service.pager;

import java.util.ArrayList;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;

abstract class AbstractQueryPager<T extends ReadQuery> implements QueryPager
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    protected final T query;

    /**
     * We cache our ReadQueries locally since some of them can short-circuit based on hitting
     * tombstone count, necessitating changes in how we determine our exhaustion
     */
    @Nullable
    protected ArrayList<ReadQuery> pageQueries;

    protected final DataLimits limits;
    protected final ProtocolVersion protocolVersion;
    private final boolean enforceStrictLiveness;
    private final int tombstonePagingThreshold = DatabaseDescriptor.getTombstonePagingThreshold();

    private int rowsRemaining;

    // This is the last key we've been reading from (or can still be reading within). This the key for
    // which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
    // (and this is done in PagerIterator). This can be null (when we start).
    private DecoratedKey lastKey;
    private int remainingInPartition;

    private boolean exhausted;

    protected AbstractQueryPager(T query, ProtocolVersion protocolVersion)
    {
        this.query = query;
        this.protocolVersion = protocolVersion;
        this.limits = query.limits();
        this.enforceStrictLiveness = query.metadata().enforceStrictLiveness();

        this.rowsRemaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();
    }

    public ReadExecutionController executionController()
    {
        return query.executionController();
    }

    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState, Dispatcher.RequestTime requestTime)
    {
        if (isExhausted())
            return EmptyIterators.partition();

        pageSize = Math.min(pageSize, rowsRemaining);
        Pager pager = new RowPager(limits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.partition();
        }
        return Transformation.apply(readQuery.execute(consistency, clientState, requestTime), pager);
    }

    public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController)
    {
        if (isExhausted())
            return EmptyIterators.partition();

        pageSize = Math.min(pageSize, rowsRemaining);
        RowPager pager = new RowPager(limits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.partition();
        }
        return Transformation.apply(readQuery.executeInternal(executionController), pager);
    }

    public UnfilteredPartitionIterator fetchPageUnfiltered(TableMetadata metadata, int pageSize, ReadExecutionController executionController)
    {
        if (isExhausted())
            return EmptyIterators.unfilteredPartition(metadata);

        pageSize = Math.min(pageSize, rowsRemaining);
        UnfilteredPager pager = new UnfilteredPager(limits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.unfilteredPartition(metadata);
        }
        return Transformation.apply(readQuery.executeLocally(executionController, tombstonePagingThreshold), pager);
    }

    private class UnfilteredPager extends Pager<Unfiltered>
    {

        private UnfilteredPager(DataLimits pageLimits, long nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        protected BaseRowIterator<Unfiltered> apply(BaseRowIterator<Unfiltered> partition)
        {
            return Transformation.apply(rowCounter.applyTo((UnfilteredRowIterator) partition), this);
        }
    }

    private class RowPager extends Pager<Row>
    {

        private RowPager(DataLimits pageLimits, long nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        protected BaseRowIterator<Row> apply(BaseRowIterator<Row> partition)
        {
            return Transformation.apply(rowCounter.applyTo((RowIterator) partition), this);
        }
    }

    private abstract class Pager<T extends Unfiltered> extends Transformation<BaseRowIterator<T>>
    {
        private final DataLimits pageLimits;
        protected final DataLimits.Counter rowCounter;
        private DecoratedKey currentKey;
        private Row lastRow;
        private boolean isFirstPartition = true;

        private Pager(DataLimits pageLimits, long nowInSec)
        {
            this.rowCounter = pageLimits.newCounter(nowInSec, true, query.selectsFullPartition(), enforceStrictLiveness);
            this.pageLimits = pageLimits;
        }

        @Override
        public BaseRowIterator<T> applyToPartition(BaseRowIterator<T> partition)
        {
            currentKey = partition.partitionKey();

            // If this is the first partition of this page, this could be the continuation of a partition we've started
            // on the previous page. In which case, we could have the problem that the partition has no more "regular"
            // rows (but the page size is such we didn't knew before) but it does has a static row. We should then skip
            // the partition as returning it would means to the upper layer that the partition has "only" static columns,
            // which is not the case (and we know the static results have been sent on the previous page).
            if (isFirstPartition)
            {
                isFirstPartition = false;
                if (isPreviouslyReturnedPartition(currentKey) && !partition.hasNext())
                {
                    partition.close();
                    return null;
                }
            }

            return apply(partition);
        }

        protected abstract BaseRowIterator<T> apply(BaseRowIterator<T> partition);

        @Override
        public void onClose()
        {
            Preconditions.checkNotNull(query, "Expected query to be non-null in Query Pager.");

            // In some case like GROUP BY a counter need to know when the processing is completed.
            rowCounter.onClose();

            recordLast(lastKey, lastRow);
            int liveRowsCounted = rowCounter.counted();
            rowsRemaining -= liveRowsCounted;

            boolean hitTombstoneLimit = false;
            // If we've paged across multiple commands and are paging across tombstones, we need to check all of them
            // and see if any of them hit a tombstone limit during graceful paging. If so, we need to snapshot what the
            // last key was on that tombstone, since the iteration over live rows that takes place here in the QueryPager
            // context won't see that tombstoned row on transformation callbacks here.
            if (tombstonePagingThreshold != -1 && pageQueries != null)
            {
                try
                {
                    for (ReadQuery cmd : pageQueries)
                    {
                        Row tsRow = cmd.tombstoneLimitedRow();
                        if (tsRow != null)
                        {
                            // Logical error: only one ReadCommand should _ever_ hit the tombstone limit
                            if (hitTombstoneLimit)
                                throw new IllegalStateException("Saw more than one ReadCommand child in a paging operation that hit their tombstone limit. This is a bug.");

                            lastRow = cmd.tombstoneLimitedRow();
                            assert lastRow != null;
                            hitTombstoneLimit = true;
                            recordLast(lastKey, lastRow);
                            break;
                        }
                    }
                }
                catch (IllegalStateException ise)
                {
                    // This shouldn't happen; snapshot the state so we have an idea of what went down
                    StringBuilder errorDetails = new StringBuilder().append("CQL Query with failure: ").append(query.toCQLString());
                    if (pageQueries != null)
                    {
                        for (ReadQuery cmd : pageQueries)
                            errorDetails.append(", ").append(cmd.toString());
                    }
                    logger.error(errorDetails.toString());
                    throw ise;
                }
            }
            if (hitTombstoneLimit)
                logger.debug("Hit tombstone limit while paging; setting clustering to resume page to: " + lastRow.clustering().toString(query.metadata()));

            // If the clustering of the last row returned is a static one, it means that the partition was only
            // containing data within the static columns. If the clustering of the last row returned is empty
            // it means that there is only one row per partition. Therefore, in both cases there are no data remaining
            // within the partition.
            if (!hitTombstoneLimit && lastRow != null &&
                (lastRow.clustering() == Clustering.STATIC_CLUSTERING || lastRow.clustering().isEmpty()))
            {
                remainingInPartition = 0;
            }
            else
            {
                remainingInPartition -= rowCounter.countedInCurrentPartition();
            }
            exhausted = !hitTombstoneLimit && pageLimits.isExhausted(rowCounter);
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
            {
                if (!currentKey.equals(lastKey))
                    remainingInPartition = limits.perPartitionCount();
                lastKey = currentKey;
                lastRow = row;
            }
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            if (!currentKey.equals(lastKey))
            {
                remainingInPartition = limits.perPartitionCount();
                lastKey = currentKey;
            }
            lastRow = row;
            return row;
        }
    }

    protected void restoreState(DecoratedKey lastKey, int remaining, int remainingInPartition)
    {
        this.lastKey = lastKey;
        this.rowsRemaining = remaining;
        this.remainingInPartition = remainingInPartition;
    }

    public boolean isExhausted()
    {
        return exhausted || rowsRemaining == 0 || ((this instanceof SinglePartitionPager) && remainingInPartition == 0);
    }

    public int maxRemaining()
    {
        return rowsRemaining;
    }

    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    protected abstract T nextPageReadQuery(int pageSize);
    protected abstract void recordLast(DecoratedKey key, Row row);
    protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey key);
}
