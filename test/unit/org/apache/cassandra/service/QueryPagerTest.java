/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.MultiPartitionPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QueryPagerTest
{
    public static final String KEYSPACE1 = "QueryPagerTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String KEYSPACE_CQL = "cql_keyspace";
    public static final String CF_CQL = "table2";
    public static final String CF_CQL_WITH_STATIC = "with_static";
    public static final String CF_CQL_WITHOUT_CLUSTERING = "without_clustering";
    public static final long nowInSec = FBUtilities.nowInSeconds();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));

        SchemaLoader.createKeyspace(KEYSPACE_CQL,
                                    KeyspaceParams.simple(1),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL + " ("
                                                               + "k text,"
                                                               + "c text,"
                                                               + "v text,"
                                                               + "PRIMARY KEY (k, c))", KEYSPACE_CQL),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL_WITH_STATIC + " ("
                                                               + "pk text, "
                                                               + "ck int, "
                                                               + "st int static, "
                                                               + "v1 int, "
                                                               + "v2 int, "
                                                               + "PRIMARY KEY(pk, ck))", KEYSPACE_CQL),
                                    CreateTableStatement.parse("CREATE TABLE " + CF_CQL_WITHOUT_CLUSTERING + " (k text PRIMARY KEY, v int)", KEYSPACE_CQL));
        addData();
    }

    private static String string(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void addData()
    {
        cfs().clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;

        // *
        // * Creates the following data:
        // *   k1: c1 ... cn
        // *   ...
        // *   ki: c1 ... cn
        // *
        for (int i = 0; i < nbKeys; i++)
        {
            for (int j = 0; j < nbCols; j++)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs().metadata(), FBUtilities.timestampMicros(), "k" + i);
                builder.clustering("c" + j).add("val", "").build().applyUnsafe();
            }
        }
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD);
    }

    private static List<FilteredPartition> query(QueryPager pager, int expectedSize)
    {
        return query(pager, expectedSize, expectedSize);
    }

    private static List<FilteredPartition> query(QueryPager pager, int toQuery, int expectedSize)
    {
        StringBuilder sb = new StringBuilder();
        List<FilteredPartition> partitionList = new ArrayList<>();
        int rows = 0;
        try (ReadExecutionController executionController = pager.executionController();
             PartitionIterator iterator = pager.fetchPageInternal(toQuery, executionController))
        {
            while (iterator.hasNext())
            {
                try (RowIterator rowIter = iterator.next())
                {
                    FilteredPartition partition = FilteredPartition.create(rowIter);
                    sb.append(partition);
                    partitionList.add(partition);
                    rows += partition.rowCount();
                }
            }
        }
        assertEquals(sb.toString(), expectedSize, rows);
        return partitionList;
    }

    private static ReadCommand namesQuery(String key, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs(), key);
        for (String name : names)
            builder.includeRow(name);
        return builder.withPagingLimit(100).build();
    }

    private static SinglePartitionReadCommand sliceQuery(String key, String start, String end, int count)
    {
        return sliceQuery(key, start, end, false, count);
    }

    private static SinglePartitionReadCommand sliceQuery(String key, String start, String end, boolean reversed, int count)
    {
        ClusteringComparator cmp = cfs().getComparator();
        TableMetadata metadata = cfs().metadata();

        Slice slice = Slice.make(cmp.make(start), cmp.make(end));
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(Slices.with(cmp, slice), reversed);

        return SinglePartitionReadCommand.create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE, Util.dk(key), filter);
    }

    private static ReadCommand rangeNamesQuery(String keyStart, String keyEnd, int count, String... names)
    {
        AbstractReadCommandBuilder builder = Util.cmd(cfs())
                                                 .fromKeyExcl(keyStart)
                                                 .toKeyIncl(keyEnd)
                                                 .withPagingLimit(count);
        for (String name : names)
            builder.includeRow(name);

        return builder.build();
    }

    private static ReadCommand rangeSliceQuery(String keyStart, String keyEnd, int count, String start, String end)
    {
        return Util.cmd(cfs())
                   .fromKeyExcl(keyStart)
                   .toKeyIncl(keyEnd)
                   .fromIncl(start)
                   .toIncl(end)
                   .withPagingLimit(count)
                   .build();
    }

    private static void assertRow(FilteredPartition r, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(r, key, bbs);
    }

    private static void assertRow(FilteredPartition partition, String key, ByteBuffer... names)
    {
        assertEquals(key, string(partition.partitionKey().getKey()));
        assertFalse(partition.isEmpty());
        int i = 0;
        for (Row row : Util.once(partition.iterator()))
        {
            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match "+string(expected)+" vs "+string(row.clustering().bufferAt(0)), expected, row.clustering().bufferAt(0));
        }
    }

    private QueryPager maybeRecreate(QueryPager pager, ReadQuery command, boolean testPagingState, ProtocolVersion protocolVersion)
    {
        if (!testPagingState)
            return pager;

        PagingState state = PagingState.deserialize(pager.state().serialize(protocolVersion), protocolVersion);
        return command.getPager(state, protocolVersion);
    }

    @Test
    public void namesQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
            namesQueryTest(protocolVersion);
    }

    public void namesQueryTest(ProtocolVersion protocolVersion)
    {
        QueryPager pager = namesQuery("k0", "c1", "c5", "c7", "c8").getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 5, 4);
        assertRow(partition.get(0), "k0", "c1", "c5", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            sliceQueryTest(false, protocolVersion);
            sliceQueryTest(true, protocolVersion);
        }
    }

    public void sliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = sliceQuery("k0", "c1", "c8", 10);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c1", "c2", "c3");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c4", "c5", "c6");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 2);
        assertRow(partition.get(0), "k0", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void reversedSliceQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            reversedSliceQueryTest(false, protocolVersion);
            reversedSliceQueryTest(true, protocolVersion);
        }
    }

    public void reversedSliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = sliceQuery("k0", "c1", "c8", true, 10);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c6", "c7", "c8");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3);
        assertRow(partition.get(0), "k0", "c3", "c4", "c5");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 2);
        assertRow(partition.get(0), "k0", "c1", "c2");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void multiQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            multiQueryTest(false, protocolVersion);
            multiQueryTest(true, protocolVersion);
        }
    }

    public void multiQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadQuery command = SinglePartitionReadCommand.Group.create(new ArrayList<SinglePartitionReadCommand>()
        {{
            add(sliceQuery("k1", "c2", "c6", 10));
            add(sliceQuery("k4", "c3", "c5", 10));
        }}, DataLimits.NONE);
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partition = query(pager, 3);
        assertRow(partition.get(0), "k1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager , 4);
        assertRow(partition.get(0), "k1", "c5", "c6");
        assertRow(partition.get(1), "k4", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partition = query(pager, 3, 1);
        assertRow(partition.get(0), "k4", "c5");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeNamesQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            rangeNamesQueryTest(false, protocolVersion);
            rangeNamesQueryTest(true, protocolVersion);
        }
    }

    public void rangeNamesQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = rangeNamesQuery("k0", "k5", 100, "c1", "c4", "c8");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 3 * 3);
        for (int i = 1; i <= 3; i++)
            assertRow(partitions.get(i-1), "k" + i, "c1", "c4", "c8");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 3 * 3, 2 * 3);
        for (int i = 4; i <= 5; i++)
            assertRow(partitions.get(i-4), "k" + i, "c1", "c4", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryTest()
    {
        for(ProtocolVersion protocolVersion : ProtocolVersion.SUPPORTED)
        {
            rangeSliceQueryTest(false, protocolVersion);
            rangeSliceQueryTest(true, protocolVersion);
        }
    }

    public void rangeSliceQueryTest(boolean testPagingState, ProtocolVersion protocolVersion)
    {
        ReadCommand command = rangeSliceQuery("k1", "k5", 100, "c1", "c7");
        QueryPager pager = command.getPager(null, protocolVersion);

        assertFalse(pager.isExhausted());
        List<FilteredPartition> partitions = query(pager, 5);
        assertRow(partitions.get(0), "k2", "c1", "c2", "c3", "c4", "c5");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 4);
        assertRow(partitions.get(0), "k2", "c6", "c7");
        assertRow(partitions.get(1), "k3", "c1", "c2");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 6);
        assertRow(partitions.get(0), "k3", "c3", "c4", "c5", "c6", "c7");
        assertRow(partitions.get(1), "k4", "c1");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), "k4", "c2", "c3", "c4", "c5", "c6");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5);
        assertRow(partitions.get(0), "k4", "c7");
        assertRow(partitions.get(1), "k5", "c1", "c2", "c3", "c4");
        assertFalse(pager.isExhausted());

        pager = maybeRecreate(pager, command, testPagingState, protocolVersion);
        assertFalse(pager.isExhausted());
        partitions = query(pager, 5, 3);
        assertRow(partitions.get(0), "k5", "c5", "c6", "c7");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void SliceQueryWithTombstoneTest()
    {
        for(ProtocolVersion version : ProtocolVersion.SUPPORTED)
            SliceQueryWithTombstoneTest(version);
    }

    public void SliceQueryWithTombstoneTest(ProtocolVersion protocolVersion)
    {
        // Testing for the bug of #6748
        String keyspace = "cql_keyspace";
        String table = "table2";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", keyspace, table, 0, i));

        ReadCommand command = SinglePartitionReadCommand.create(cfs.metadata(), nowInSec, Util.dk("k0"), Slice.ALL);

        QueryPager pager = command.getPager(null, protocolVersion);

        for (int i = 0; i < 5; i++)
        {
            List<FilteredPartition> partitions = query(pager, 1);
            // The only live cell we should have each time is the row marker
            assertRow(partitions.get(0), "k0", "c" + i);
        }
    }

    @Test
    public void pagingReversedQueriesWithStaticColumnsTest()
    {
        // There was a bug in paging for reverse queries when the schema includes static columns in
        // 2.1 & 2.2. This was never a problem in 3.0, so this test just guards against regressions
        // see CASSANDRA-13222

        // insert some rows into a single partition
        for (int i=0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (pk, ck, st, v1, v2) VALUES ('k0', %3$s, %3$s, %3$s, %3$s)",
                                          KEYSPACE_CQL, CF_CQL_WITH_STATIC, i));

        // query the table in reverse with page size = 1 & check that the returned rows contain the correct cells
        TableMetadata table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL_WITH_STATIC).metadata();
        queryAndVerifyCells(table, true, "k0");
    }

    private void queryAndVerifyCells(TableMetadata table, boolean reversed, String key)
    {
        ClusteringIndexFilter rowfilter = new ClusteringIndexSliceFilter(Slices.ALL, reversed);
        ReadCommand command = SinglePartitionReadCommand.create(table, nowInSec, Util.dk(key), ColumnFilter.all(table), rowfilter);
        QueryPager pager = command.getPager(null, ProtocolVersion.CURRENT);

        ColumnMetadata staticColumn = table.staticColumns().getSimple(0);
        assertEquals(staticColumn.name.toCQLString(), "st");

        for (int i=0; i<5; i++)
        {
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator partitions = pager.fetchPageInternal(1, controller))
            {
                try (RowIterator partition = partitions.next())
                {
                    assertCell(partition.staticRow(), staticColumn, 4);

                    Row row = partition.next();
                    int cellIndex = !reversed ? i : 4 - i;

                    assertEquals(row.clustering().bufferAt(0), ByteBufferUtil.bytes(cellIndex));
                    assertCell(row, table.getColumn(new ColumnIdentifier("v1", false)), cellIndex);
                    assertCell(row, table.getColumn(new ColumnIdentifier("v2", false)), cellIndex);

                    // the partition/page should contain just a single regular row
                    assertFalse(partition.hasNext());
                }
            }
        }

        // After processing the 5 rows there should be no more rows to return
        try ( ReadExecutionController controller = pager.executionController();
              PartitionIterator partitions = pager.fetchPageInternal(1, controller))
        {
            assertFalse(partitions.hasNext());
        }
    }

    private void assertCell(Row row, ColumnMetadata column, int value)
    {
        Cell<?> cell = row.getCell(column);
        assertNotNull(cell);
        assertEquals(value, ByteBufferUtil.toInt(cell.buffer()));
    }

    @Test
    public void tombstoneCheckpointDoesNotReplaceLaterCursor()
    {
        verifyTombstoneCheckpoint(false);
    }

    @Test
    public void localRangePagerRejectsTombstoneCheckpoint()
    {
        int paging = DatabaseDescriptor.getTombstonePagingThreshold();
        int failure = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            String key = "range_checkpoint_rejection";
            checkpointQuery(key, 3, false, DataLimits.NONE);
            ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
            for (boolean unfiltered : new boolean[]{ false, true })
            {
                QueryPager pager = ((PartitionRangeReadCommand) Util.cmd(table).fromKeyIncl(key).toKeyIncl(key).build())
                                  .withUpdatedLimit(DataLimits.cqlLimits(10)).getPager(null, ProtocolVersion.CURRENT);
                assertThatThrownBy(() -> {
                    try (ReadExecutionController controller = pager.executionController())
                    {
                        if (unfiltered)
                        {
                            try (UnfilteredPartitionIterator partitions = ((PartitionRangeQueryPager) pager).fetchPageUnfiltered(table.metadata(), 8, controller))
                            {
                                while (partitions.hasNext())
                                    try (UnfilteredRowIterator rows = partitions.next())
                                    {
                                        while (rows.hasNext())
                                            rows.next();
                                    }
                            }
                        }
                        else
                        {
                            readPageRows(pager, 8);
                        }
                    }
                }).isInstanceOf(org.apache.cassandra.db.filter.TombstoneOverwhelmingException.class);
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(paging);
            DatabaseDescriptor.setTombstoneFailureThreshold(failure);
        }
    }

    @Test
    public void successfulCheckpointIsNotATombstoneFailure()
    {
        int paging = DatabaseDescriptor.getTombstonePagingThreshold();
        int warning = DatabaseDescriptor.getTombstoneWarnThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneWarnThreshold(1);
            SinglePartitionReadCommand command = checkpointQuery("checkpoint_metrics", 3, false, DataLimits.NONE);
            ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
            long failures = table.metric.tombstoneFailures.getCount();
            long warnings = table.metric.tombstoneWarnings.getCount();
            SinglePartitionPager pager = command.getPager(null, ProtocolVersion.CURRENT);
            assertTrue(readPageRows(pager, 8).isEmpty());
            assertFalse(pager.isExhausted());
            assertEquals(failures, table.metric.tombstoneFailures.getCount());
            assertEquals(warnings + 1, table.metric.tombstoneWarnings.getCount());
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(paging);
            DatabaseDescriptor.setTombstoneWarnThreshold(warning);
        }
    }

    @Test
    public void smallPagingThresholdsMakeProgress()
    {
        int original = DatabaseDescriptor.getTombstonePagingThreshold();
        try
        {
            for (int threshold : new int[]{ -2, 0, 1, 2 })
            {
                DatabaseDescriptor.setTombstonePagingThreshold(threshold);
                for (boolean reversed : new boolean[]{ false, true })
                {
                    String key = "small_threshold_" + threshold + '_' + reversed;
                    SinglePartitionPager pager = checkpointQuery(key, 4, reversed, DataLimits.cqlLimits(3))
                                                 .getPager(null, ProtocolVersion.CURRENT);
                    List<String> rows = new ArrayList<>();
                    for (int page = 0; !pager.isExhausted(); page++)
                    {
                        assertTrue("Paging did not terminate at threshold " + threshold, page < 7);
                        rows.addAll(readPageRows(pager, 2));
                    }
                    assertEquals(reversed ? Arrays.asList(key + "/c1", key + "/c0")
                                          : Arrays.asList(key + "/c4", key + "/c5"), rows);
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(original);
        }
    }

    @Test
    public void markerOnlyScanHasBoundedThresholdOverrun()
    {
        int failure = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(4);
            String key = "marker_scan_limit";
            for (int i = 0; i < 2200; i += 2)
                executeInternal("DELETE FROM " + KEYSPACE_CQL + '.' + CF_CQL +
                                " USING TIMESTAMP 2 WHERE k = ? AND c >= ? AND c < ?",
                                key, String.format("c%04d", i), String.format("c%04d", i + 1));
            ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
            SinglePartitionReadCommand command = (SinglePartitionReadCommand) Util.cmd(table, key).build();
            command.trackWarnings();
            MessageParams.reset();
            long failures = table.metric.tombstoneFailures.getCount();
            assertThatThrownBy(() -> {
                try (ReadExecutionController controller = command.executionController();
                     UnfilteredPartitionIterator partitions = command.executeLocally(controller, 2))
                {
                    while (partitions.hasNext())
                        try (UnfilteredRowIterator rows = partitions.next())
                        {
                            while (rows.hasNext())
                                rows.next();
                        }
                }
            }).isInstanceOf(org.apache.cassandra.db.filter.TombstoneOverwhelmingException.class)
              .hasMessageContaining("Scanned over 1027 tombstones");
            assertEquals(failures + 1, table.metric.tombstoneFailures.getCount());
            assertEquals(Integer.valueOf(1027), MessageParams.get(ParamType.TOMBSTONE_FAIL));
            assertNull(command.tombstoneLimitedRow());
        }
        finally
        {
            DatabaseDescriptor.setTombstoneFailureThreshold(failure);
            MessageParams.reset();
        }
    }

    @Test
    public void tombstoneCheckpointSurvivesPagingStateRoundTrip()
    {
        verifyTombstoneCheckpoint(true);
    }

    private void verifyTombstoneCheckpoint(boolean recreate)
    {
        int pagingThreshold = DatabaseDescriptor.getTombstonePagingThreshold();
        int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
                for (boolean reversed : new boolean[]{ false, true })
                    for (int deleted : new int[]{ 3, 6 })
                    {
                        String key = "checkpoint_" + recreate + '_' + version + '_' + reversed + '_' + deleted;
                        SinglePartitionReadCommand command = checkpointQuery(key, deleted, reversed, DataLimits.cqlLimits(5));
                        QueryPager pager = command.getPager(null, version);
                        for (int scanned = 3; scanned <= deleted; scanned += 3)
                        {
                            query(pager, 1, 0);
                            assertFalse(pager.isExhausted());
                            assertEquals(5, pager.maxRemaining());
                            int ck = reversed ? deleted + 2 - scanned : scanned - 1;
                            assertEquals("c" + ck, string(pager.state().rowMark.clustering(command.metadata()).bufferAt(0)));
                            pager = maybeRecreate(pager, command, recreate, version);
                        }
                        for (int i = 0; i < 2; i++)
                        {
                            int ck = reversed ? 1 - i : deleted + i;
                            List<FilteredPartition> partitions = query(pager, 1, 1);
                            assertRow(partitions.get(0), key, "c" + ck);
                            assertFalse(pager.isExhausted());
                            assertEquals(4 - i, pager.maxRemaining());
                            assertEquals("c" + ck, string(pager.state().rowMark.clustering(command.metadata()).bufferAt(0)));
                            pager = maybeRecreate(pager, command, recreate, version);
                        }
                        query(pager, 1, 0);
                        assertTrue(pager.isExhausted());
                        assertEquals(3, pager.maxRemaining());
                    }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(pagingThreshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        }
    }

    @Test
    public void unfilteredTombstoneCheckpointDoesNotPreventExhaustion()
    {
        int pagingThreshold = DatabaseDescriptor.getTombstonePagingThreshold();
        int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            SinglePartitionReadCommand command = checkpointQuery("unfiltered_checkpoint", 3, false, DataLimits.NONE);
            SinglePartitionPager pager = command.getPager(null, ProtocolVersion.CURRENT);
            for (int page = 0; page < 2; page++)
            {
                int expected = page == 0 ? 0 : 3;
                try (ReadExecutionController controller = pager.executionController();
                     UnfilteredPartitionIterator partitions = pager.fetchPageUnfiltered(command.metadata(), 8, controller))
                {
                    assertTrue(partitions.hasNext());
                    try (UnfilteredRowIterator partition = partitions.next())
                    {
                        while (partition.hasNext())
                        {
                            Unfiltered item = partition.next();
                            assertTrue(item.isRow());
                            Row row = (Row) item;
                            assertEquals(bytes("c" + expected++), row.clustering().bufferAt(0));
                            assertEquals(page != 0, row.deletion().isLive());
                        }
                    }
                    assertFalse(partitions.hasNext());
                }
                assertEquals(page == 0 ? 3 : 5, expected);
                assertEquals(page != 0, pager.isExhausted());
                if (page == 0)
                    assertEquals(bytes("c2"), pager.state().rowMark.clustering(command.metadata()).bufferAt(0));
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(pagingThreshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        }
    }

    @Test(timeout = 30000)
    public void aggregationJoinsInternalSubPages()
    {
        int threshold = DatabaseDescriptor.getTombstonePagingThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            for (boolean groupBy : new boolean[]{ false, true })
                for (boolean reversed : new boolean[]{ false, true })
                    for (int shape = 0; shape < 3; shape++)
                    {
                        DataLimits limits = groupBy
                                            ? DataLimits.groupByLimits(100, 100, 2, AggregationSpecification.aggregatePkPrefixFactory(cfs().getComparator(), 0)
                                                                                                          .newInstance(QueryOptions.DEFAULT))
                                            : DataLimits.cqlLimits(100);
                        SinglePartitionReadCommand first = sliceQuery("k1", "c0", "c9", reversed, 10).withUpdatedLimit(limits);
                        SinglePartitionReadCommand second = sliceQuery("k2", "c0", "c9", reversed, 10).withUpdatedLimit(limits);
                        AbstractReadCommandBuilder range = Util.cmd(cfs()).fromKeyIncl("k1").toKeyIncl("k2");
                        if (reversed)
                            range.reverse();
                        ReadQuery command = shape == 0 ? first
                                            : shape == 1 ? SinglePartitionReadCommand.Group.create(Arrays.asList(first, second), limits)
                                                         : ((PartitionRangeReadCommand) range.build()).withUpdatedLimit(limits);
                        QueryPager pager = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), limits);
                        assertFalse(pager.isTopK());
                        assertEquals(100, pager.maxRemaining());
                        List<String> expected = new ArrayList<>();
                        for (int key = 1; key <= (shape == 0 ? 1 : 2); key++)
                            for (int i = 0; i < 10; i++)
                                expected.add("k" + key + "/c" + (reversed ? 9 - i : i));

                        List<String> actual = new ArrayList<>();
                        for (int page = 0; !pager.isExhausted(); page++)
                        {
                            assertTrue("Aggregation did not finish", page < 3);
                            List<String> rows = readPageRows(pager, groupBy ? 1 : 2);
                            assertTrue(rows.toString(), rows.size() <= (groupBy ? 10 : expected.size()));
                            actual.addAll(rows);
                            if (!pager.isExhausted())
                            {
                                PagingState state = PagingState.deserialize(pager.state().serialize(ProtocolVersion.CURRENT), ProtocolVersion.CURRENT);
                                pager = new AggregationQueryPager(command.getPager(state, ProtocolVersion.CURRENT), limits);
                            }
                        }
                        assertEquals("groupBy=" + groupBy + ", reversed=" + reversed + ", shape=" + shape, expected, actual);
                        assertEquals(100 - (groupBy ? (shape == 0 ? 1 : 2) : expected.size()), pager.maxRemaining());
                    }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(threshold);
        }
    }

    @Test(timeout = 30000)
    public void aggregationContinuesAcrossEmptyTombstoneSubPages()
    {
        int threshold = DatabaseDescriptor.getTombstonePagingThreshold();
        int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            for (boolean reversed : new boolean[]{ false, true })
            {
                String key = "aggregate_checkpoint_" + reversed;
                SinglePartitionReadCommand command = checkpointQuery(key, 6, reversed, DataLimits.cqlLimits(5));
                ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
                long crossings = table.metric.tombstonePages.getCount();
                QueryPager pager = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), command.limits());
                assertNull(pager.state());
                assertEquals(Arrays.asList(key + (reversed ? "/c1" : "/c6"), key + (reversed ? "/c0" : "/c7")), readPageRows(pager, 1));
                assertTrue(pager.isExhausted());
                assertEquals(3, pager.maxRemaining());
                assertEquals(2, table.metric.tombstonePages.getCount() - crossings);
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(threshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        }
    }

    @Test(timeout = 30000)
    public void groupedAggregationContinuesAfterLiveRowCheckpoint()
    {
        verifyGroupedAggregationAfterLiveRowCheckpoint(false);
    }

    @Test(timeout = 30000)
    public void reversedGroupedAggregationContinuesAfterLiveRowCheckpoint()
    {
        verifyGroupedAggregationAfterLiveRowCheckpoint(true);
    }

    private void verifyGroupedAggregationAfterLiveRowCheckpoint(boolean reversed)
    {
        int threshold = DatabaseDescriptor.getTombstonePagingThreshold();
        int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
            DataLimits limits = DataLimits.groupByLimits(10, 10, 2, AggregationSpecification.aggregatePkPrefixFactory(table.getComparator(), 0)
                                                                                          .newInstance(QueryOptions.DEFAULT));
            String key = "grouped_checkpoint_" + reversed;
            SinglePartitionReadCommand command = checkpointQuery(key, 6, reversed, limits);
            executeInternal("INSERT INTO " + KEYSPACE_CQL + '.' + CF_CQL + " (k, c, v) VALUES (?, ?, 'v') USING TIMESTAMP 3",
                            key, reversed ? "c7" : "c0");
            List<String> expected = Arrays.asList(key + (reversed ? "/c7" : "/c0"), key + (reversed ? "/c1" : "/c6"), key + (reversed ? "/c0" : "/c7"));
            DatabaseDescriptor.setTombstonePagingThreshold(-1);
            DatabaseDescriptor.setTombstoneFailureThreshold(100);
            QueryPager control = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), limits);
            assertEquals(expected, readPageRows(control, 1));
            assertEquals(9, control.maxRemaining());
            if (!control.isExhausted())
                assertTrue(readPageRows(control, 1).isEmpty());
            assertTrue(control.isExhausted());
            assertEquals(9, control.maxRemaining());

            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            long crossings = table.metric.tombstonePages.getCount();
            QueryPager pager = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), limits);
            List<String> actual = readPageRows(pager, 1);
            assertTrue("Expected a tombstone checkpoint", table.metric.tombstonePages.getCount() > crossings);
            assertEquals(expected, actual);
            assertEquals(9, pager.maxRemaining());
            if (!pager.isExhausted())
                assertTrue(readPageRows(pager, 1).isEmpty());
            assertTrue(pager.isExhausted());
            assertEquals(9, pager.maxRemaining());
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(threshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        }
    }

    @Test
    public void rangePagerStopsAfterFinalStaticPartition()
    {
        String key = "final_static_partition";
        executeInternal("UPDATE " + KEYSPACE_CQL + '.' + CF_CQL_WITH_STATIC + " SET st = 1 WHERE pk = ?", key);
        ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL_WITH_STATIC);
        PartitionRangeReadCommand command = ((PartitionRangeReadCommand) Util.cmd(table).fromKeyIncl(key).toKeyIncl(key).build())
                                            .withUpdatedLimit(DataLimits.cqlLimits(10, 1));
        QueryPager initial = command.getPager(null, ProtocolVersion.CURRENT);
        try (ReadExecutionController controller = initial.executionController();
             PartitionIterator page = initial.fetchPageInternal(1, controller))
        {
            assertTrue(page.hasNext());
            try (RowIterator partition = page.next())
            {
                assertEquals(bytes(key), partition.partitionKey().getKey());
                assertEquals(bytes(1), partition.staticRow().getCell(table.metadata().getColumn(bytes("st"))).buffer());
                assertFalse(partition.hasNext());
            }
            assertFalse(page.hasNext());
        }
        assertFalse(initial.isExhausted());
        PagingState state = initial.state();
        assertEquals(9, state.remaining);
        assertEquals(0, state.remainingInPartition);
        assertNull(state.rowMark);
        for (int readPath = 0; readPath < 3; readPath++)
        {
            PartitionRangeQueryPager pager = (PartitionRangeQueryPager) command.getPager(state, ProtocolVersion.CURRENT);
            try (ReadExecutionController controller = pager.executionController())
            {
                if (readPath == 2)
                {
                    try (UnfilteredPartitionIterator page = pager.fetchPageUnfiltered(table.metadata(), 1, controller))
                    {
                        assertFalse(page.hasNext());
                    }
                }
                else
                {
                    try (PartitionIterator page = readPath == 0 ? pager.fetchPageInternal(1, controller)
                                                               : pager.fetchPage(1, ConsistencyLevel.ONE, ClientState.forInternalCalls(),
                                                                                 Dispatcher.RequestTime.forImmediateExecution()))
                    {
                        assertFalse(page.hasNext());
                    }
                }
            }
            assertTrue(pager.isExhausted());
            assertEquals(9, pager.maxRemaining());
        }
    }

    @Test
    public void rangePagingPreservesInclusiveAndExclusiveBounds()
    {
        for (boolean includeStart : new boolean[]{ false, true })
            for (boolean includeEnd : new boolean[]{ false, true })
                for (int perPartition : new int[]{ 1, 3 })
                    for (int pageSize : new int[]{ 1, 2 })
                    {
                        AbstractReadCommandBuilder.PartitionRangeBuilder builder = Util.cmd(cfs());
                        if (includeStart)
                            builder.fromKeyIncl("k1");
                        else
                            builder.fromKeyExcl("k1");
                        if (includeEnd)
                            builder.toKeyIncl("k3");
                        else
                            builder.toKeyExcl("k3");
                        PartitionRangeReadCommand command = ((PartitionRangeReadCommand) builder.fromIncl("c1").toIncl("c3").build())
                                                            .withUpdatedLimit(DataLimits.cqlLimits(100, perPartition));
                        QueryPager pager = command.getPager(null, ProtocolVersion.CURRENT);
                        assertNull(pager.state());
                        List<String> expected = new ArrayList<>();
                        for (int key = includeStart ? 1 : 2; key <= (includeEnd ? 3 : 2); key++)
                            for (int ck = 1; ck <= perPartition; ck++)
                                expected.add("k" + key + "/c" + ck);
                        List<String> actual = new ArrayList<>();
                        for (int page = 0; !pager.isExhausted(); page++)
                        {
                            assertTrue("Range pager did not finish", page <= expected.size());
                            List<String> rows = readPageRows(pager, pageSize);
                            assertTrue(rows.size() <= pageSize);
                            actual.addAll(rows);
                            if (!pager.isExhausted())
                                pager = maybeRecreate(pager, command, true, ProtocolVersion.CURRENT);
                        }
                        assertEquals(expected, actual);
                        assertEquals(100 - expected.size(), pager.maxRemaining());
                    }
    }

    @Test
    public void aggregationIsEmptyContinuesAcrossTombstoneSubPages()
    {
        int threshold = DatabaseDescriptor.getTombstonePagingThreshold();
        int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL);
            for (boolean groupBy : new boolean[]{ false, true })
                for (boolean reversed : new boolean[]{ false, true })
                {
                    String key = "aggregation_empty_" + groupBy + '_' + reversed;
                    DataLimits limits = groupBy
                                        ? DataLimits.groupByLimits(5, 5, 2, AggregationSpecification.aggregatePkPrefixFactory(table.getComparator(), 0)
                                                                                                 .newInstance(QueryOptions.DEFAULT))
                                        : DataLimits.cqlLimits(5);
                    SinglePartitionReadCommand command = checkpointQuery(key, 6, reversed, limits);
                    String first = reversed ? "c7" : "c0";
                    executeInternal("INSERT INTO " + KEYSPACE_CQL + '.' + CF_CQL +
                                    " (k, c, v) VALUES (?, ?, 'v') USING TIMESTAMP 3", key, first);
                    QueryPager pager = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), limits);
                    long crossings = table.metric.tombstonePages.getCount();
                    List<String> actual = new ArrayList<>();
                    try (ReadExecutionController controller = pager.executionController();
                         PartitionIterator page = pager.fetchPageInternal(1, controller))
                    {
                        assertTrue(page.hasNext());
                        try (RowIterator partition = page.next())
                        {
                            while (!partition.isEmpty())
                            {
                                assertTrue("Aggregation exceeded the live-row count", actual.size() < 3);
                                assertFalse(partition.isEmpty());
                                assertTrue(partition.hasNext());
                                actual.add(string(partition.next().clustering().bufferAt(0)));
                            }
                            assertTrue(partition.isEmpty());
                            assertFalse(partition.hasNext());
                        }
                        assertFalse(page.hasNext());
                    }
                    assertEquals(reversed ? Arrays.asList("c7", "c1", "c0") : Arrays.asList("c0", "c6", "c7"), actual);
                    if (!pager.isExhausted())
                        assertTrue(readPageRows(pager, 1).isEmpty());
                    assertTrue(pager.isExhausted());
                    assertTrue("Expected a tombstone checkpoint", table.metric.tombstonePages.getCount() > crossings);
                }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(threshold);
            DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        }
    }

    @Test
    public void rangePagingResumesLegacyStaticPartitionBoundary()
    {
        String first = "legacy_static_a";
        String second = "legacy_static_b";
        String tableName = KEYSPACE_CQL + '.' + CF_CQL_WITH_STATIC;
        executeInternal("UPDATE " + tableName + " SET st = 1 WHERE pk = ?", first);
        executeInternal("UPDATE " + tableName + " SET st = 2 WHERE pk = ?", second);
        ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL_WITH_STATIC);
        PartitionRangeReadCommand command = ((PartitionRangeReadCommand) Util.cmd(table).fromKeyIncl(first).toKeyIncl(second).build())
                                            .withUpdatedLimit(DataLimits.cqlLimits(10, 1));
        QueryPager pager = command.getPager(null, ProtocolVersion.V3);
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator page = pager.fetchPageInternal(1, controller))
        {
            assertTrue(page.hasNext());
            try (RowIterator partition = page.next())
            {
                assertEquals(bytes(first), partition.partitionKey().getKey());
                assertFalse(partition.hasNext());
            }
            assertFalse(page.hasNext());
        }
        PagingState state = pager.state();
        assertNull(state.rowMark);
        assertEquals(0, state.remainingInPartition);
        ByteBuffer legacy = state.serialize(ProtocolVersion.V3);
        // Cassandra 2.1/2.2 paging states omit the final per-partition count.
        legacy.limit(legacy.limit() - Integer.BYTES);
        PagingState restored = PagingState.deserialize(legacy, ProtocolVersion.V3);
        assertEquals(Integer.MAX_VALUE, restored.remainingInPartition);
        pager = command.getPager(restored, ProtocolVersion.V3);
        List<String> keys = new ArrayList<>();
        for (int pages = 0; !pager.isExhausted(); pages++)
        {
            assertTrue("Range pager did not finish", pages < 3);
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator page = pager.fetchPageInternal(1, controller))
            {
                while (page.hasNext())
                {
                    try (RowIterator partition = page.next())
                    {
                        keys.add(string(partition.partitionKey().getKey()));
                        assertEquals(bytes(2), partition.staticRow().getCell(table.metadata().getColumn(bytes("st"))).buffer());
                        assertFalse(partition.hasNext());
                    }
                }
            }
        }
        assertEquals(Arrays.asList(second), keys);
        assertEquals(8, pager.maxRemaining());
    }

    @Test
    public void aggregationIteratorMetadataAndEarlyClose()
    {
        for (boolean groupBy : new boolean[]{ false, true })
            for (boolean reversed : new boolean[]{ false, true })
            {
                DataLimits limits = groupBy
                                    ? DataLimits.groupByLimits(100, 100, 2, AggregationSpecification.aggregatePkPrefixFactory(cfs().getComparator(), 0)
                                                                                                  .newInstance(QueryOptions.DEFAULT))
                                    : DataLimits.cqlLimits(100);
                ReadQuery command = SinglePartitionReadCommand.Group.create(Arrays.asList(sliceQuery("k1", "c0", "c9", reversed, 10).withUpdatedLimit(limits),
                                                                                         sliceQuery("k2", "c0", "c9", reversed, 10).withUpdatedLimit(limits)), limits);
                QueryPager pager = new AggregationQueryPager(command.getPager(null, ProtocolVersion.CURRENT), limits);
                assertThatThrownBy(() -> pager.withUpdatedLimit(DataLimits.NONE)).isInstanceOf(UnsupportedOperationException.class);
                OpOrder.Barrier barrier = cfs().readOrdering.newBarrier();
                try (ReadExecutionController controller = pager.executionController())
                {
                    barrier.issue();
                    try (PartitionIterator page = pager.fetchPageInternal(0, controller))
                    {
                        assertTrue(page.hasNext());
                        try (RowIterator partition = page.next())
                        {
                            assertEquals(cfs().metadata(), partition.metadata());
                            assertEquals(cfs().metadata().regularAndStaticColumns(), partition.columns());
                            assertEquals(reversed, partition.isReverseOrder());
                            assertTrue(partition.staticRow().isEmpty());
                            assertFalse(partition.isEmpty());
                            assertEquals(bytes("k1"), partition.partitionKey().getKey());
                            assertEquals(bytes(reversed ? "c9" : "c0"), partition.next().clustering().bufferAt(0));
                        }
                    }
                    // The caller owns the controller, including when iteration stops early.
                    assertFalse(barrier.getSyncPoint().isFinished());
                }
                assertTrue(barrier.getSyncPoint().isFinished());
            }
    }

    @Test
    public void pagerExhaustsWithoutClusteringColumns()
    {
        String key = "no_clustering";
        executeInternal("INSERT INTO " + KEYSPACE_CQL + '.' + CF_CQL_WITHOUT_CLUSTERING + " (k, v) VALUES (?, 1)", key);
        ColumnFamilyStore table = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL_WITHOUT_CLUSTERING);
        ReadCommand command = Util.cmd(table, key).withLimit(5).build();
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
        {
            QueryPager pager = command.getPager(null, version);
            try (ReadExecutionController controller = pager.executionController();
                 PartitionIterator page = pager.fetchPageInternal(1, controller))
            {
                assertTrue(page.hasNext());
                try (RowIterator partition = page.next())
                {
                    assertEquals(bytes(key), partition.partitionKey().getKey());
                    assertTrue(partition.hasNext());
                    Row row = partition.next();
                    assertTrue(row.clustering().isEmpty());
                    assertEquals(bytes(1), row.getCell(table.metadata().getColumn(bytes("v"))).buffer());
                    assertFalse(partition.hasNext());
                }
                assertFalse(page.hasNext());
            }
            assertTrue(pager.isExhausted());
            assertEquals(4, pager.maxRemaining());
        }
    }

    @Test
    public void exhaustedPagersReturnEmptyPages()
    {
        SinglePartitionPager single = sliceQuery("k1", "c1", "c1", 1).withUpdatedLimit(DataLimits.cqlLimits(1))
                                                                            .getPager(null, ProtocolVersion.CURRENT);
        assertEquals(Arrays.asList("k1/c1"), readPageRows(single, 1));
        for (QueryPager pager : Arrays.asList(QueryPager.EMPTY, single))
        {
            assertTrue(pager.isExhausted());
            assertEquals(0, pager.maxRemaining());
            assertTrue(readPageRows(pager, 1).isEmpty());
            try (PartitionIterator page = pager.fetchPage(1, ConsistencyLevel.ONE, ClientState.forInternalCalls(),
                                                         Dispatcher.RequestTime.forImmediateExecution()))
            {
                assertFalse(page.hasNext());
            }
        }
        try (ReadExecutionController controller = single.executionController();
             UnfilteredPartitionIterator page = single.fetchPageUnfiltered(cfs().metadata(), 1, controller))
        {
            assertFalse(page.hasNext());
        }
        assertNull(QueryPager.EMPTY.state());
        assertThatThrownBy(() -> QueryPager.EMPTY.withUpdatedLimit(DataLimits.NONE)).isInstanceOf(UnsupportedOperationException.class);

        SinglePartitionReadCommand.Group group = SinglePartitionReadCommand.Group.create(Arrays.asList(sliceQuery("k1", "c1", "c2", 2),
                                                                                                       sliceQuery("k2", "c1", "c2", 2)), DataLimits.NONE);
        QueryPager missingKey = new MultiPartitionPager<>(group, new PagingState(bytes("not_in_query"), null, 5, 5), ProtocolVersion.CURRENT);
        assertTrue(missingKey.isExhausted());
        assertNull(missingKey.state());
        try (ReadExecutionController controller = ReadExecutionController.empty();
             PartitionIterator page = missingKey.fetchPageInternal(1, controller))
        {
            assertFalse(page.hasNext());
        }
    }

    private static List<String> readPageRows(QueryPager pager, int pageSize)
    {
        List<String> rows = new ArrayList<>();
        try (ReadExecutionController controller = pager.executionController();
             PartitionIterator page = pager.fetchPageInternal(pageSize, controller))
        {
            while (page.hasNext())
            {
                try (RowIterator partition = page.next())
                {
                    while (partition.hasNext())
                    {
                        assertTrue("Page exceeded the fixture's row count", rows.size() < 100);
                        rows.add(string(partition.partitionKey().getKey()) + '/' + string(partition.next().clustering().bufferAt(0)));
                    }
                }
            }
            assertThatThrownBy(page::next).isInstanceOf(NoSuchElementException.class);
        }
        return rows;
    }

    private static SinglePartitionReadCommand checkpointQuery(String key, int deleted, boolean reversed, DataLimits limits)
    {
        String table = KEYSPACE_CQL + '.' + CF_CQL;
        for (int i = 0; i < deleted + 2; i++)
        {
            String ck = "c" + (reversed ? deleted + 1 - i : i);
            executeInternal("INSERT INTO " + table + " (k, c, v) VALUES (?, ?, 'v') USING TIMESTAMP 1", key, ck);
            if (i < deleted)
                executeInternal("DELETE FROM " + table + " USING TIMESTAMP 2 WHERE k = ? AND c = ?", key, ck);
        }
        TableMetadata metadata = Keyspace.open(KEYSPACE_CQL).getColumnFamilyStore(CF_CQL).metadata();
        return SinglePartitionReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), RowFilter.none(),
                                                 limits, Util.dk(key), new ClusteringIndexSliceFilter(Slices.ALL, reversed));
    }
}
