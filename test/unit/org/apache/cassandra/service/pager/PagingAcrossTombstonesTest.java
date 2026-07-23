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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.RandomHelpers;

import static org.apache.cassandra.service.pager.AbstractPagingValidator.AsyncTombstonePagingValidator;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.COL;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.CellState;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.ClusteringBoundType;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.ClusteringBounds;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.DEFAULT_FAILURE_THRESHOLD;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.LivenessState;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.ROW_TARGET;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.STATIC_COL;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.SyncTombstonePagingValidator;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.TRACE;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.TRACE_EXPECTED;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.TRACE_SEEN;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.liveCellCount;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.trace;
import static org.apache.cassandra.utils.RandomHelpers.randomFromRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class PagingAcrossTombstonesTest extends CQLTester
{
    protected static final int UNINIT = Integer.MIN_VALUE;

    private final String testName;
    private final AbstractPagingValidator validator;
    private Session session;

    // Parameterized runner invokes the static @Parameters method (forcing class init) before @BeforeClass runs,
    // so DatabaseDescriptor.conf is null unless we initialize it here, ahead of the ORIGINAL_* field reads below.
    static { DatabaseDescriptor.daemonInitialization(); }

    private static final int ORIGINAL_PAGE_ACROSS = DatabaseDescriptor.getTombstonePagingThreshold();
    private static final int ORIGINAL_FAILURE_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    private static final int ORIGINAL_WARNING_THRESHOLD = DatabaseDescriptor.getTombstoneWarnThreshold();

    private static final String SYNC_FORWARD = "SyncPageTombstonesForward";
    private static final String SYNC_REVERSE = "SyncPageTombstonesReverse";
    private static final String ASYNC_FORWARD = "AsyncPageTombstonesForward";
    private static final String ASYNC_REVERSE = "AsyncPageTombstonesReverse";

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> pagers()
    {
        return Arrays.asList(new Object[][]{
            { SYNC_FORWARD, new SyncTombstonePagingValidator(UNINIT, UNINIT, false) },
            { SYNC_REVERSE, new SyncTombstonePagingValidator(UNINIT, UNINIT, true) },
            { ASYNC_FORWARD, new AsyncTombstonePagingValidator(UNINIT, UNINIT, false) },
            { ASYNC_REVERSE, new AsyncTombstonePagingValidator(UNINIT, UNINIT, true) }
            });
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setTombstonePagingThreshold(25000);
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setTombstonePagingThreshold(ORIGINAL_PAGE_ACROSS);
        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_FAILURE_THRESHOLD);
        DatabaseDescriptor.setTombstoneWarnThreshold(ORIGINAL_WARNING_THRESHOLD);
    }

    @Before
    public void beforeTest()
    {
        if (session == null)
        {
            session = sessionNet();
            validator.setSession(session);
        }
        validator.beforeTest();
        validator.reset();
    }

    public PagingAcrossTombstonesTest(String name, AbstractPagingValidator validator)
    {
        this.testName = name;
        this.validator = validator;
    }

    @Test
    public void testSimpleGeneration() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);
        validator.generateLinear(0, 100);
        validator.runTest();
    }

    /**
     * Confirm that in the base case of requesting a page with only static data and nothing else, we get it back.
     */
    @Test
    public void testWithOnlyStaticData() throws ExecutionException, InterruptedException, TimeoutException
    {
        TRACE=true;
        TRACE_SEEN=true;
        TRACE_EXPECTED=true;
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        for (int i = 0; i < 10; i++)
        {
            validator.generateStaticData();
            validator.deleteStaticData();
        }
        validator.generateStaticData();

        validator.runTest();
        assertTrue(validator.staticDataSeen());
    }

    /**
     * Base case where we are checking to ensure we don't throw a {@link TombstoneOverwhelmingException} when configured to page across tombstones.
     */
    @Test
    public void testLinearScanOverLimit() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateStaticData();

        validator.generateLinear(0, ROW_TARGET);

        // Insert enough tombstones to blow past our failure threshold
        validator.deleteLinear(0, ROW_TARGET, LivenessState.CELL_TOMBSTONE);

        int liveDataCount = 10;
        // Insert live data beyond the threshold where we'd traditionally get shut down by a TombstoneOverwhelmingException
        validator.generateLinear(ROW_TARGET, liveDataCount);

        validator.runTest();

        // Work with range 2000-3000 for this next set; confirm that we skip the empty range correctly. This is a bit
        // of a sanity check for the internals of our expected model calculation rather than anything particular to the
        // pager.
        int offset = 2000;

        // Test with very small page size
        validator.prepareTest(10, DEFAULT_FAILURE_THRESHOLD);

        // And we want to add more data and tombstone out, with cell and row tombstones, a middle chunk
        validator.generateLinear(offset, 1000);

        // Pull out half the data in the middle
        validator.deleteLinear(offset + 250, 250, LivenessState.CELL_TOMBSTONE);
        validator.deleteLinear(offset + 500, 250, LivenessState.ROW_TOMBSTONE);

        // And with a much larger page size
        validator.prepareTest(2000, DEFAULT_FAILURE_THRESHOLD);
        validator.runTest();

        assertTrue(validator.staticDataSeen());
    }

    /**
     * Confirm that in the case with all rows deleted, paging sync across the data succeeds and returns the right values (none),
     * but that it also does it in one shot trying to fill the designated fetch size.
     */
    @Test
    public void testWithAllRowDeletions() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(300, DEFAULT_FAILURE_THRESHOLD);
        validator.generateLinear(0, ROW_TARGET);
        validator.runTest();

        validator.prepareTest(300, DEFAULT_FAILURE_THRESHOLD);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.ROW_TOMBSTONE);
        validator.runTest();
    }

    /**
     * Confirm that the driver will spin through all the pages on attempts to hit the synchronous API, giving us no data
     * back as expected but also not missing iteration across any rows in the partition.
     */
    @Test
    public void testWithAllCellTombstones() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateStaticData();
        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.CELL_TOMBSTONE);

        validator.runTest();
    }

    /**
     * Moving into mixed territory with row tombstones; pulling out the first 50 from every block of 100 w/failure threshold
     * 100 to make sure that some more complex paging is working correctly. This is strictly redundant with some of the other
     * tests but useful scaffolding as we build up correctness.
     */
    @Test
    public void testMixedPageTypes() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);

        // Delete the first 50 clusterings on each block of 100. This will give us a mix of full and short-circuited pages.
        for (int block = 0; block < 1000; block += 100)
            for (int i = 0; i < 50; i++)
                validator.deleteRow(block + i);

        validator.runTest();
    }

    /**
     * In the presence of a mix of cell, row, and range tombstones, confirm that the synchronous driver API calls correctly
     * pages across the data, hitting everything, and that the live / tombstoned cells match the model.
     *
     * When using the async API in the presence of cell, row, and range tombstones that surpass our failure threshold,
     * ensure that the async API gives us full control over hitting our tombstone threshold and doesn't spin trying to
     * fill the {@link ResultSet} from the fetch size set on the pager.
     *
     * Here we're putting more stress on the model to calculate expected tombstones from randomized input data that overlaps
     * with a range tombstone.
     */
    @Test
    public void testMixedTombstones() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(25, DEFAULT_FAILURE_THRESHOLD);
        validator.generateLinear(0, ROW_TARGET);

        // We randomly delete 100 cells and 100 rows, making sure they're mutually exclusive clusterings from one another.
        validator.deleteRandomized(100, 0, 1000, LivenessState.CELL_TOMBSTONE);
        validator.deleteRandomized(100, 0, 1000, LivenessState.ROW_TOMBSTONE);

        validator.runTest();

        validator.prepareTest(25, DEFAULT_FAILURE_THRESHOLD);
        // Single range tombstone at the upper third
        validator.deleteRange(350, ClusteringBoundType.EXCLUSIVE, 700, ClusteringBoundType.EXCLUSIVE, validator.getNextTimestamp());

        // Testing with small fetch size
        validator.prepareTest(10, DEFAULT_FAILURE_THRESHOLD);
        validator.runTest();

        // Testing this scenario with large fetch size that may trigger short-circuiting
        validator.prepareTest(ROW_TARGET, DEFAULT_FAILURE_THRESHOLD);
        validator.runTest();
    }

    /**
     * Confirm we short-circuit when working with a combination of cell and row tombstones, and we limit ourselves to a total
     * of N tombstones in total and not of just one kind.
     */
    @Test
    public void testShortCircuitOnBothTypesOfTombstones() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);
        validator.generateStaticData();
        validator.generateLinear(0, ROW_TARGET);

        // First 50 rows == live data we'll get
        validator.deleteLinear(50, 50, LivenessState.ROW_TOMBSTONE);
        validator.deleteLinear(100, 50, LivenessState.CELL_TOMBSTONE);

        validator.runTest();
    }

    /**
     * Range tombstones shouldn't trigger short-circuits on paging
     */
    @Test
    public void testRangeTombstoneDoesntShortCircuit() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);
        validator.generateLinear(0, ROW_TARGET);

        ClusteringBounds toDelete = new ClusteringBounds(50, ClusteringBoundType.INCLUSIVE, 350, ClusteringBoundType.EXCLUSIVE);
        validator.deleteRange(toDelete, validator.getNextTimestamp());

        validator.runTest();
    }

    /**
     * Range tombstones that overlap with cell or row tombstones should still not trigger short-circuits on paging
     */
    @Test
    public void testRangeTombstoneOverlapsDontShortCircuit() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);

        validator.deleteLinear(50, 150, LivenessState.CELL_TOMBSTONE);
        validator.deleteLinear(200, 150, LivenessState.ROW_TOMBSTONE);
        validator.deleteRange(50, 350);

        validator.runTest();
    }

    /**
     * Test the boundary condition where we expect to both hit our tombstone limit but also exhaust the pager on the same
     * clustering. Previous logic only used the calculation of live row sizes in the {@link AbstractQueryPager.Pager#onClose} to
     * determine whether the pager was done with its work as we want to ensure that the new logic of using the
     * tombstoned clustering authoritatively doesn't run afoul of logic in the java driver.
     */
    @Test
    public void testExhaustionOverlap() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, 1000);
        validator.deleteLinear(900, 100, LivenessState.ROW_TOMBSTONE);

        // We expect to see a short-circuit on that last tombstone even if it was the limit and there's no data left.
        // However, we also expect we didn't try and query _another_ page after that. 9 pages, 1 short-circuited.
        validator.runTest();

        // Strictly speaking, these assertions are redundant with the internal checks in the validator.
        assertEquals(900, liveCellCount(validator.seenCellState));
    }

    /**
     * Test the case where the vast majority of data is very sparse and we expect to hit our tombstone limits frequently
     */
    @Test
    public void testSparseLiveData() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(25, 10);

        validator.generateStaticData();
        validator.generateLinear(0, 100);
        validator.deleteLinear(0, 100, LivenessState.ROW_TOMBSTONE);
        validator.generateRandomized(5, 0, 100);
        validator.printExpectedLive(CellState::isRowLive);

        validator.runTest();
        assertEquals(5, liveCellCount(validator.seenCellState));
    }

    @Test
    public void testRandomizedData() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(75, 25);
        validator.generateRandomized(300, 300, ROW_TARGET);
        validator.deleteRandomized(300, 150, ROW_TARGET, LivenessState.ROW_TOMBSTONE);
        validator.deleteRandomized(300, 150, ROW_TARGET, LivenessState.CELL_TOMBSTONE);

        int s = randomFromRange(0, ROW_TARGET);
        int e = randomFromRange(s, s + randomFromRange(15, 50));
        validator.deleteRange(s, e);

        validator.toggleReversed();

        validator.runTest();
    }

    /**
     * Confirm that, in the presence of a range tombstone, if we terminate on that we don't end up blowing up.
     */
    @Test
    public void testCellStopOnRangeTombstone() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, 200, LivenessState.CELL_TOMBSTONE);
        validator.deleteRange(0, 110);

        validator.runTest();
    }

    @Test
    public void testRowStopOnRangeTombstone() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, 200, LivenessState.ROW_TOMBSTONE);
        validator.deleteRange(0, 110);

        validator.runTest();
    }

    @Test
    public void testStaticDataWithAllCellDeletion() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateStaticData();
        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.CELL_TOMBSTONE);

        validator.runTest();

        assertTrue(validator.staticDataExpected());
        assertTrue(validator.staticDataSeen());
    }

    @Test
    public void testStaticDataWithAllRowDeletions() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateStaticData();
        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.ROW_TOMBSTONE);

        validator.runTest();

        assertTrue(validator.staticDataExpected());
        assertTrue(validator.staticDataSeen());
    }

    /**
     * Confirm that static column tombstones trigger paging but also that it short-circuits when it's a tombstone counted.
     */
    @Test
    public void testStaticShortCircuits() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, 99, LivenessState.CELL_TOMBSTONE);
        assertTrue(validator.generateStaticData());
        assertTrue(validator.deleteStaticData());

        validator.runTest();

        assertFalse(validator.staticDataExpected());
        assertFalse(validator.staticDataSeen());
    }

    /**
     * Confirm that all four tombstone types, when combined, will lead to a short-circuit event in sum.
     */
    @Test
    public void testAllTombstonesCombined() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);

        validator.deleteStaticData();

        validator.deleteLinear(0, 25, LivenessState.CELL_TOMBSTONE);
        validator.deleteLinear(25, 72, LivenessState.ROW_TOMBSTONE);

        validator.deleteRange(25 + 72, 25 + 73);
        validator.deleteRange(25 + 73, 25 + 74);

        validator.runTest();
    }

    @Test
    public void testInsertionInsideRangeTombstone() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.deleteRange(0, 1000);
        validator.generateLinear(150, 150);

        validator.runTest();
        assertEquals(150, validator.totalSeenLiveCells());
    }

    @Test
    public void testInsertionAfterMixedTombstones() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(50, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);

        validator.deleteLinear(0, 300, LivenessState.CELL_TOMBSTONE);
        validator.deleteLinear(300, 300, LivenessState.ROW_TOMBSTONE);
        validator.deleteRange(600, ROW_TARGET);

        validator.runTest();
        assertEquals(0, validator.totalSeenLiveCells());

        validator.prepareTest(50, DEFAULT_FAILURE_THRESHOLD);
        validator.generateLinear(350, 150);
        validator.runTest();
        assertEquals(150, validator.totalSeenLiveCells());
    }

    @Test
    public void testRangeTombstonedOverBound() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteRange(1500, ROW_TARGET + 500);
        validator.deleteRange(-10, 100);

        validator.runTest();
    }

    /**
     * Confirm things come through as expected if all the data is tombstoned
     */
    @Test
    public void testRangeTombstonedComplete() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.ROW_TOMBSTONE);

        // Generate some more data in the middle w/a newer TS and then RT it
        validator.generateLinear(400, 250);
        validator.deleteRange(350, 700);

        validator.runTest();
    }

    @Test
    public void testAlmostAllRangeTombstoned() throws ExecutionException, InterruptedException, TimeoutException
    {
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, ROW_TARGET);
        validator.deleteLinear(0, ROW_TARGET, LivenessState.ROW_TOMBSTONE);
        validator.generateLinear(400, 250);
        validator.deleteRange(350, 500);
        validator.deleteRange(501, 651);

        validator.runTest();
        validator.printExpectedLive(CellState::isRowLive);
        assertEquals(1, validator.expectedLiveRowCount());
    }

    @SuppressWarnings("SingleCharacterStringConcatenation")
    @Test
    public void testTombstoneLimitInCollection() throws ExecutionException, InterruptedException, TimeoutException
    {
        // Skip on Sync validator parameterization; manually running both
        assumeTrue(this.testName.equals(ASYNC_FORWARD));

        String table = "page_collection_test";
        validator.session.execute(String.format("CREATE TABLE %s (key text, clustering int, %s text, %s text static, test_map map<text, int>, PRIMARY KEY (key, clustering));",
                                      table,
                                      COL,
                                      STATIC_COL));

        for (int i = 0; i < 10; i++)
        {
            StringBuilder sb = new StringBuilder()
                               .append(String.format("INSERT INTO %s (key, clustering, %s, test_map) VALUES (", table, COL))
                               .append(String.format("'key', %d, '%s', ", i, RandomHelpers.makeRandomString(50)));
            sb.append('{');
            for (int j = 0; j < 20; j++)
            {
                sb.append("'key").append(j).append("': ").append(j);
                if (j != 19)
                    sb.append(", ");
            }
            sb.append("});");
            trace(sb.toString());
            validator.session.execute(sb.toString());
        }

        // We're going to delete a set of 10 within a map in the middle, set our threshold of failure to 5, and have it page.
        StringBuilder rb = new StringBuilder().append(String.format("UPDATE %s SET test_map = test_map - {", table));
        for (int i = 0; i < 10; i++)
        {
            rb.append("'key").append(i).append("'");
            if (i != 9)
                rb.append(", ");
        }
        rb.append("} WHERE key = 'key' AND clustering = 5;");
        trace(rb.toString());
        validator.session.execute(rb.toString());

        // Confirm we see all the contents we expect to see in all the maps populated above
        DatabaseDescriptor.setTombstoneFailureThreshold(5);

        String query = String.format("SELECT * FROM %s WHERE key = 'key'", table);
        SimpleStatement statement = new SimpleStatement(query);
        statement.setFetchSize(10);
        ResultSet rs = session.execute(statement);

        Set<Integer> seenClusterings = new HashSet<>();
        for (Row r : rs.all())
        {
            seenClusterings.add(r.getInt("clustering"));
            int expectedMapSize = r.getInt("clustering") == 5 ? 10 : 20;
            assertEquals(expectedMapSize, r.getMap("test_map", String.class, Integer.class).size());
        }
        assertEquals(10, seenClusterings.size());

        // Now do the same as above just using an async querying paradigm
        statement = new SimpleStatement(String.format("SELECT * FROM %s WHERE key = 'key'", table));
        statement.setFetchSize(10);
        rs = session.executeAsync(statement).getUninterruptibly();

        seenClusterings.clear();
        while (rs.getExecutionInfo().getPagingState() != null)
        {
            checkCollection(rs, seenClusterings);
            rs = rs.fetchMoreResults().get(5, TimeUnit.SECONDS);
        }
        checkCollection(rs, seenClusterings);
        assertEquals(10, seenClusterings.size());
    }

    private void checkCollection(ResultSet rs, Set<Integer> seenClusterings)
    {
        int liveRows = rs.getAvailableWithoutFetching();
        for (int i = 0; i < liveRows; i++)
        {
            Row r = rs.one();
            seenClusterings.add(r.getInt("clustering"));
            int expectedMapSize = r.getInt("clustering") == 5 ? 10 : 20;
            assertEquals(expectedMapSize, r.getMap("test_map", String.class, Integer.class).size());
        }
    }

    /**
     * Not only should we get client warnings, but they should piggyback the {@link NoSpamLogger} so they don't spam the client.
     */
    @Test
    public void testClientWarnings() throws Throwable
    {
        // Not using the validator querying for this test
        assumeTrue(this.testName.equals(ASYNC_FORWARD));

        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
        execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", KEYSPACE));

        String table = "test_warn_table";
        execute(String.format("CREATE TABLE %s.%s (pk int, c int, v text, PRIMARY KEY (pk, c))", KEYSPACE, table));

        DatabaseDescriptor.setTombstoneFailureThreshold(10);

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, ProtocolVersion.CURRENT))
        {
            client.connect(false);
            client.execute(new QueryMessage(String.format("USE %s;", KEYSPACE), QueryOptions.DEFAULT));

            for (int i = 0; i < 1000; i++)
                execute(String.format("INSERT INTO %s.%s (pk, c, v) VALUES (1, %d, '%s')", KEYSPACE, table, i, RandomHelpers.makeRandomString(50)));

            for (int i = 200; i < 300; i++)
                execute(String.format("DELETE FROM %s.%s WHERE pk = 1 AND c = %d;", KEYSPACE, table, i));

            QueryMessage query = new QueryMessage(String.format("SELECT v FROM %s.%s WHERE pk = 1 AND c = 1;", KEYSPACE, table), QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            assertNull(resp.getWarnings());

            query = new QueryMessage(String.format("SELECT * FROM %s.%s WHERE pk = 1;", KEYSPACE, table), QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertNotNull("Response was unexpectedly null.", resp);
            assertNotNull("Response warnings was unexpectedly null.", resp.getWarnings());
            assertEquals(1, resp.getWarnings().size());
        }
    }
}