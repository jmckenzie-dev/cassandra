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
package org.apache.cassandra.index.sasi.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.sasi.disk.RowKey;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;

public class QueryPlan
{
    private final QueryController controller;

    public QueryPlan(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs)
    {
        this.controller = new QueryController(cfs, (PartitionRangeReadCommand) command, executionQuotaMs);
    }

    /**
     * Converts expressions into operation tree (which is currently just a single AND).
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        try
        {
            Operation.Builder and = new Operation.Builder(OperationType.AND, controller);
            controller.getExpressions().forEach(and::add);
            return and.complete();
        }
        catch (Exception | Error e)
        {
            controller.finish();
            throw e;
        }
    }

    public UnfilteredPartitionIterator execute(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return new ResultIterator(analyze(), controller, executionController);
    }

    private static class ResultIterator implements UnfilteredPartitionIterator
    {
        private final AbstractBounds<PartitionPosition> keyRange;
        private final Operation operationTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;

        private Iterator<RowKey> currentKeys = null;
        private UnfilteredRowIterator nextPartition = null;
        // TODO: this is a mess!
        private DecoratedKey lastPartitionKey = null;
        private Row lastStaticRow = null;

        public ResultIterator(Operation operationTree, QueryController controller, ReadExecutionController executionController)
        {
            this.keyRange = controller.dataRange().keyRange();
            this.operationTree = operationTree;
            this.controller = controller;
            this.executionController = executionController;

            if (operationTree != null)
                operationTree.skipTo((Long) keyRange.left.getToken().getTokenValue());
        }

        public boolean hasNext()
        {
            return prepareNext();
        }

        public UnfilteredRowIterator next()
        {
            if (nextPartition == null)
                prepareNext();

            UnfilteredRowIterator toReturn = nextPartition;
            nextPartition = null;
            return toReturn;
        }

        private boolean prepareNext()
        {
            if (operationTree == null)
                return false;

            if (nextPartition != null)
                nextPartition.close();

            for (;;)
            {
                if (currentKeys == null || !currentKeys.hasNext())
                {
                    if (!operationTree.hasNext())
                        return false;

                    Token token = operationTree.next();
                    currentKeys = token.iterator();
                }

                TableMetadata metadata = controller.metadata();
                Set<RowKey> rows = new TreeSet<>();
                // results have static clustering, the whole partition has to be read
                boolean fetchWholePartition = false;

                while (true)
                {
                    if (!currentKeys.hasNext())
                    {
                        // No more keys for this token.
                        // If no clusterings were collected yet, exit this inner loop so the operation
                        // tree iterator can move on to the next token.
                        // If some clusterings were collected, build an iterator for those rows
                        // and return.
                        if ((rows.isEmpty() && !fetchWholePartition) || lastPartitionKey == null)
                            break;

                        UnfilteredRowIterator partition = new FakePartitionIterator(metadata, operationTree, lastPartitionKey, lastStaticRow, rows);

                        // Prepare for next partition, reset partition key and clusterings
                        lastPartitionKey = null;
                        lastStaticRow = null;
                        rows = new TreeSet<RowKey>();

                        if (partition.isEmpty())
                        {
                            partition.close();
                            continue;
                        }

                        nextPartition = partition;
                        return true;
                    }

                    RowKey fullKey = currentKeys.next();
                    DecoratedKey key = fullKey.decoratedKey;

                    if (!keyRange.right.isMinimum() && keyRange.right.compareTo(key) < 0)
                        return false;

                    if (lastPartitionKey != null && metadata.partitionKeyType.compare(lastPartitionKey.getKey(), key.getKey()) != 0)
                    {
                        UnfilteredRowIterator partition = new FakePartitionIterator(metadata, operationTree, lastPartitionKey,lastStaticRow, rows);

                        if (partition.isEmpty())
                            partition.close();
                        else
                        {
                            nextPartition = partition;
                            return true;
                        }
                    }

                    lastPartitionKey = key;
                    lastStaticRow = fullKey.staticRow;

                    // We fetch whole partition for versions before AC and in case static column index is queried in AC
                    if (fullKey.clustering == null || fullKey.clustering.clustering().kind() == ClusteringPrefix.Kind.STATIC_CLUSTERING)
                        fetchWholePartition = true;
                    else if (controller.dataRange().clusteringIndexFilter(fullKey.decoratedKey).selects(fullKey.clustering))
                    {
                        rows.add(fullKey);
                    }

                }
            }
        }

        private UnfilteredRowIterator fetchPartition(DecoratedKey key, NavigableSet<Clustering> clusterings, boolean fetchWholePartition)
        {
            if (fetchWholePartition)
                clusterings = null;

            try (UnfilteredRowIterator partition = controller.getPartition(key, clusterings, executionController))
            {
                Row staticRow = partition.staticRow();
                List<Unfiltered> clusters = new ArrayList<>();

                while (partition.hasNext())
                {
                    Unfiltered row = partition.next();
                    if (operationTree.satisfiedBy(row, staticRow, true))
                        clusters.add(row);
                }

                if (!clusters.isEmpty())
                    return new PartitionIterator(partition, clusters);
                else
                    return UnfilteredRowIterators.noRowsIterator(partition.metadata(),
                                                                 partition.partitionKey(),
                                                                 Rows.EMPTY_STATIC_ROW,
                                                                 partition.partitionLevelDeletion(),
                                                                 partition.isReverseOrder());
            }
        }

        public void close()
        {
            if (nextPartition != null)
                nextPartition.close();

            FileUtils.closeQuietly(operationTree);
            controller.finish();
        }

        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Collection<Unfiltered> filteredRows)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      partition.staticRow(),
                      partition.isReverseOrder(),
                      partition.stats());

                rows = filteredRows.iterator();
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }

        private static class FakePartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<RowKey> rows;
            private final Operation operation;

            public FakePartitionIterator(TableMetadata metaData, Operation operation, DecoratedKey pk, Row staticRow, Collection<RowKey> partition)
            {
                super(metaData,
                      pk,
                      DeletionTime.LIVE,
                      metaData.regularAndStaticColumns(),
                      // TODO: static columns
                      staticRow,
                      false,
                      EncodingStats.NO_STATS);
                rows = partition.iterator();
                this.operation = operation;
            }

            @Override
            protected Unfiltered computeNext()
            {
                while (rows.hasNext())
                {
                    Unfiltered unfiltered = rows.next().unfiltered();

                    if (operation.satisfiedBy(unfiltered, staticRow, true))
                        return unfiltered;

                }

                return endOfData();
            }
        }
    }
}
