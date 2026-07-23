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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import junit.framework.AssertionFailedError;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.utils.RandomHelpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This is a utility class to help perform fuzz testing against pagers within the Cassandra Codebase. The primary usage
 * pattern is:
 *
 * validator.beforeTest() // allows multiple tests to share the same validator
 * validator.prepareTest(...)
 * validator.generateN(...)
 * validator.deleteN(...)
 * validator.toggleReversed()
 * validator.toggleStatic()
 * validator.runTest()
 *
 * The various generation methods will build a local model via {@link CellState} objects in the {@link #expectedCellState}
 * collection. Then, during {@link #runTest()}, the client will query the database using the specified paging size and
 * failure threshold specified in {@link #prepareTest}, iterating through the results and building the {@link #seenCellState}
 * collection. The diff of these 2 collections is our authoritative confirmation that paging worked as expected or failed.
 *
 * There's a collection of unused methods in the class that have proven useful during debug and inspection of results.
 * We're intentionally leaving those in for now for future maintainers.
 */
public abstract class AbstractPagingValidator
{
    public static boolean TRACE_CQL = false;
    public static int TRACE_CLUSTERING = -1;

    protected static final int UNINIT = Integer.MIN_VALUE;

    /** For detailed information on internal state; see {@link #trace} */
    public static boolean TRACE = false;
    public static boolean TRACE_SEEN = false;
    public static boolean TRACE_EXPECTED = false;

    /** For detailed test results */
    private static boolean VERBOSE_RESULTS = true;

    private static final Logger logger = LoggerFactory.getLogger(AbstractPagingValidator.class);

    static final int DEFAULT_FAILURE_THRESHOLD = 100;

    // Don't end on a divisible page boundary by default
    static final int ROW_TARGET = DEFAULT_FAILURE_THRESHOLD * 10 + 5;

    final static String USER_SEED = "cassandra.paging_test.seed";

    protected Session session;

    protected final static String KEYSPACE = "page_tombstone_test";
    protected final static String TABLE = "page_tombstone_table";
    protected final static String COL = "col";

    protected final static String CLUSTERING = "clustering";
    protected final static String STATIC_COL = "stat";
    protected final static int STATIC_CLUSTERING = Integer.MIN_VALUE;

    protected boolean reversed;
    private static long nextTS;

    public int pageSize = UNINIT;
    public int failureThreshold = UNINIT;

    protected CellState expectedStaticCell = CellState.EMPTY_STATIC;
    protected CellState seenStaticCell = CellState.EMPTY_STATIC;

    /** The cell state for the internal model */
    protected final Map<Integer, CellState> expectedCellState = new HashMap<>();
    /** The cell state built based on what we receive over the client connection */
    protected final Map<Integer, CellState> seenCellState = new HashMap<>();

    private enum TraceType
    {
        GENERAL,
        EXPECTED,
        SEEN,
        CQL
    }

    public void beforeTest()
    {
        nextTS = 1;

        reset();

        executeKeyspace("DROP KEYSPACE IF EXISTS %s");
        executeKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        executeKeyspace("USE %s");
        session.execute(String.format("CREATE TABLE %s (key text, clustering int, %s text, %s text static, PRIMARY KEY (key, clustering));",
                                      TABLE,
                                      COL,
                                      STATIC_COL));
    }

    public AbstractPagingValidator(int pageSize, int failureThreshold, boolean reversed)
    {
        this.pageSize = pageSize;
        this.failureThreshold = failureThreshold;
        this.reversed = reversed;
        DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
    }

    public void setSession(Session session)
    {
        this.session = session;
    }

    /**
     * If called without a parameter, will run with the same value in {@link #reversed} as the previous run.
     */
    public void runTest() throws ExecutionException, InterruptedException, TimeoutException
    {
        runTest(this.reversed);
    }

    /**
     * Runs the test, persists the value passed in as reversed, and runs validation that the cells retrieved from
     * the Database match the cells calculated as expected during data generation.
     */
    public void runTest(boolean reversed) throws ExecutionException, InterruptedException, TimeoutException
    {
        this.reversed = reversed;
        queryData();

        List<AssertionError> errors = new ArrayList<>();

        captureError(() -> validateCellState(expectedCellState, seenCellState, staticDataSeen()), errors);
        captureError(() -> assertEquals("Unexpected state of static data.", expectedStaticCell.state, seenStaticCell.state), errors);

        if (errors.size() == 0 && VERBOSE_RESULTS)
        {
            logger.info("\n\n[TEST RESULT DETAILS]\n" + this);
        }

        if (errors.size() != 0)
        {
            logger.error("\n\n[FAILING TEST DETAILS]\n" + this);
            for (AssertionError ae : errors)
                logger.error("Failure in test: " + ae.getMessage());
            throw new AssertionError("Saw >= 1 failure in test. See log for details.");
        }
    }

    public abstract void queryData() throws ExecutionException, InterruptedException, TimeoutException;

    /**
     * Exposed for {@link PagingFuzzTest}
     */
    public void toggleReversed()
    {
        reversed = !reversed;
    }

    /**
     * Exposed for {@link PagingFuzzTest}
     */
    public void toggleStatic()
    {
        if (staticDataExpected())
            deleteStaticData();
        else
            generateStaticData();
    }

    protected SimpleStatement generateQueryStatement()
    {
        String query = String.format("SELECT * FROM %s WHERE key = 'key'", TABLE);
        if (reversed)
            query += " ORDER BY clustering DESC";
        return new SimpleStatement(query);
    }

    /**
     * Keeps the {@link #expectedCellState} that's merged in with the model but clears all record of seen state
     */
    void prepareTest(int newPageSize, int failureThreshold)
    {
        this.pageSize = newPageSize;
        this.failureThreshold = failureThreshold;
        DatabaseDescriptor.setTombstoneFailureThreshold(failureThreshold);
        seenCellState.clear();
        seenStaticCell = CellState.EMPTY_STATIC;
    }

    /**
     * Completely resets all expected and seen state between tests
     */
    void reset()
    {
        prepareTest(UNINIT, UNINIT);
        expectedCellState.clear();
        expectedStaticCell = CellState.EMPTY_STATIC;
    }

    private void mergeExpected(Map<Integer, CellState> toMerge)
    {
        if (toMerge.size() == 0)
            return;

        boolean shouldChange = false;
        int dc = CellState.DEBUG_CLUSTERING;
        StringBuilder sb = null;
        // Debugging only here
        if (dc != -1)
        {
            sb = new StringBuilder();
            appendLine(sb, "\n\n----[ Cell merge on: " + dc + "]----");
            CellState preState = expectedCellState.get(dc);
            appendLine(sb, preState == null
                           ? "DEBUG_CLUSTERING not found in prelim expected state: " + dc
                           : "DEBUG_CLUSTERING data in prelim expected state: " + preState);

            CellState mergeState = toMerge.get(dc);
            appendLine(sb, mergeState == null
                           ? "DEBUG_CLUSTERING not found in merge state: " + dc
                           : "DEBUG_CLUSTERING data in merge state: " + mergeState);
            if (preState == null && mergeState != null)
                shouldChange = true;
            else if (preState != null && mergeState != null && preState.timestamp <= mergeState.timestamp)
                shouldChange = true;
        }

        // Update if new state w/new timestamp or same w/overlapping precedence
        expectedCellState.forEach((data, state) -> state.updateState(toMerge.get(data)));

        // Insert if totally new state
        traceExpected("expectedCount pre merge of new: " + expectedCellState.size());
        toMerge.forEach(expectedCellState::putIfAbsent);
        traceExpected("expectedCount post merge of new: " + expectedCellState.size());

        // A touch more debug printing and tracing for single cell / logic debugging
        if (shouldChange)
        {
            CellState postState = expectedCellState.get(dc);
            assertNotNull(postState);
            appendLine(sb, "DEBUG_CLUSTERING data in post merged state: " + postState);
            trace(sb.toString(), TraceType.EXPECTED);
        }
    }

    boolean staticDataExpected()
    {
        return expectedStaticCell.isCellLive();
    }

    boolean staticDataSeen()
    {
        return seenStaticCell.isCellLive();
    }

    public static void validateCellState(Map<Integer, CellState> expected, Map<Integer, CellState> seen, boolean staticDataSeen)
    {
        StringBuilder errors = new StringBuilder();

        int minClustering = seen.size() == 0
                            ? Integer.MIN_VALUE
                            : seen.keySet().stream().min(Comparator.comparingInt(o -> o)).get();

        for (CellState e : expected.values())
        {
            CellState s = seen.get(e.clustering);

            if (LivenessState.sentToClient(e.state))
            {
                if (s == null || e.state != s.state)
                    errors.append("Mismatched state. ").append(cellDiff(e, s)).append('\n');
            }
            // We have one edge case where we have static data and no other live data; in this case we can only
            // deduce client side that the cell should be tombstoned, not what specific _type_ of tombstone that is.
            else if (s != null)
            {
                if (s.clustering != minClustering || !staticDataSeen)
                    errors.append("Client saw cell that should have been range / row tombstoned. ").append(cellDiff(e, s)).append('\n');
            }
        }

        for (CellState s : seen.values())
        {
            if (!expected.containsKey(s.clustering))
            {
                if (s.clustering != minClustering || !staticDataSeen)
                    errors.append("Client saw unexpected cell. Seen: ").append(s).append('\n');
            }
        }
        if (errors.length() != 0)
        {
            logger.error("Error during cell validation.");
            throw new AssertionError("Failed to validate Cell State. Printing out mismatched cells:\n" + errors);
        }
    }

    /**
     * Wraps up and collects assertions so we can have multiple failures checked on a given test run rather than short-circuit
     */
    public void captureError(Runnable function, Collection<AssertionError> errors)
    {
        try
        {
            function.run();
        }
        catch (AssertionFailedError error)
        {
            errors.add(error);
        }
    }

    private static String cellDiff(CellState expected, CellState seen)
    {
        return "Expected: " + expected + ", Seen: " + seen;
    }

    enum LivenessState
    {
        LIVE,
        CELL_TOMBSTONE,
        ROW_TOMBSTONE,
        RANGE_TOMBSTONE,
        UNINITIALIZED;

        /**
         * When clients query the DB, there's a variety of things we filter out on the DB side
         * that don't get sent back to the client. Due to this, there's things we just won't see
         * on the client side when we query, so we can't build a 1:1 model client-side of what's
         * going on server-side.
         * @return Whether the input state is expected to be sent to the client
         */
        public static boolean sentToClient(LivenessState state)
        {
            return state == LIVE || state == CELL_TOMBSTONE;
        }
    }

    enum ClusteringBoundType
    {
        INCLUSIVE,
        EXCLUSIVE
    }

    static class ClusteringBounds
    {
        public static final ClusteringBounds INVALID_CLUSTERING_BOUNDS = new ClusteringBounds(Integer.MIN_VALUE,
                                                                                              ClusteringBoundType.INCLUSIVE,
                                                                                              Integer.MIN_VALUE,
                                                                                              ClusteringBoundType.INCLUSIVE);

        public int minClustering;
        public ClusteringBoundType minClusteringType;
        public int maxClustering;
        public ClusteringBoundType maxClusteringType;

        public ClusteringBounds(int minClustering, ClusteringBoundType minClusteringType, int maxClustering, ClusteringBoundType maxClusteringType)
        {
            this.minClustering = minClustering;
            this.minClusteringType = minClusteringType;
            this.maxClustering = maxClustering;
            this.maxClusteringType = maxClusteringType;
        }

        public boolean contains(ClusteringBounds other)
        {
            int actualMin = minClusteringType == ClusteringBoundType.INCLUSIVE ? minClustering - 1 : minClustering;
            int actualMax = maxClusteringType == ClusteringBoundType.INCLUSIVE ? maxClustering + 1 : maxClustering;

            return other.minClustering > actualMin && other.maxClustering < actualMax;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(minClustering, minClusteringType, maxClustering, maxClusteringType);
        }

        @Override
        public boolean equals(Object rhs)
        {
            if (rhs == null)
                return false;
            if (!(rhs instanceof ClusteringBounds))
                return false;

            ClusteringBounds other = (ClusteringBounds)rhs;
            return minClustering == other.minClustering &&
                   minClusteringType == other.minClusteringType &&
                   maxClustering == other.maxClustering &&
                   maxClusteringType == other.maxClusteringType;
        }

        @Override
        public String toString()
        {
            return "minClustering: " + minClustering + ", minClusteringType: " + minClusteringType + ", maxClustering: " + maxClustering + ", maxClusteringType: " + maxClusteringType;
        }
    }

    /**
     * This is a pretty truncated view of what's in a {@link Cell}. We need to know if the data
     * is alive or dead and its timestamp for reconciliation; the support of different data types
     * and data isn't relevant to our desire to exercise paging and ensure the data is there, correct,
     * and has the right liveness.
     */
    static class CellState
    {
        public static boolean TRACE_CELL_STATE = false;

        public static final CellState EMPTY_STATIC = new CellState(-1, "null", LivenessState.UNINITIALIZED, Integer.MIN_VALUE);

        static int DEBUG_CLUSTERING = -1;
        public final int clustering;
        private String data;
        private LivenessState state;
        private long timestamp;
        private @Nullable ClusteringBounds rangeTombstone = null;

        public CellState(int clustering, String data, LivenessState state, long timestamp)
        {
            this.clustering = clustering;
            this.data = data;
            this.state = state;
            this.timestamp = timestamp;
        }

        public void setRangeTombstone(ClusteringBounds bounds)
        {
            rangeTombstone = bounds;
        }

        public static CellState fromRow(Row r)
        {
            return new CellState(r.getInt(CLUSTERING),
                                 (r.isNull(COL) ? "null" : r.getString(COL)),
                                 r.isNull(COL) ? LivenessState.CELL_TOMBSTONE : LivenessState.LIVE,
                                 -1);
        }

        /**
         * If the other state is == the timestamp of this, then the delete wins.
         * see {@link Cells#resolveRegular}
         */
        public void updateState(@Nullable CellState other)
        {
            if (other == null)
            {
                cellTrace("OTHER NULL");
                return;
            }
            cellTrace("self on enter updateState: " + this);
            cellTrace("other on enter updateState: " + other);

            cellTrace("self timestamp: " + timestamp + ". Other timestamp: " + other.timestamp);

            if (timestamp > other.timestamp)
            {
                cellTrace("KEEPING ME");
                return;
            }
            cellTrace("UPDATING TO THEM");

            state = other.state;
            if (state != LivenessState.LIVE)
                data = "null";
            timestamp = other.timestamp;
            cellTrace("self on exit updateState: " + this);
        }

        public boolean isCellLive()
        {
            return state == LivenessState.LIVE;
        }

        public boolean isRowLive()
        {
            return state != LivenessState.ROW_TOMBSTONE && state != LivenessState.RANGE_TOMBSTONE;
        }

        // Cell tombstoned rows are, all else being equal, still returned to clients
        public boolean isCellTombstoned()
        {
            return state == LivenessState.CELL_TOMBSTONE;
        }

        // Row tombstoned data isn't returned to clients but counts toward short-circuiting pages
        public boolean isRowTombstoned()
        {
            return state == LivenessState.ROW_TOMBSTONE;
        }

        // Range tombstones aren't returned to clients and are only counted once toward short-circuiting pages
        public boolean isRangeTombstoned()
        {
            return state == LivenessState.RANGE_TOMBSTONE;
        }

        public boolean isNonRangeTombstoned()
        {
            return isCellTombstoned() || isRowTombstoned();
        }

        @VisibleForTesting
        public LivenessState getState()
        {
            return state;
        }

        /**
         * In the case where we have no live data excepting a static column, we don't know until after we've processed
         * our entire queried dataset and need to set the state of this clustering value after creation.
         */
        public void setRowTombstoned()
        {
            state = LivenessState.ROW_TOMBSTONE;
        }

        public ClusteringBounds getRangeTombstone()
        {
            return rangeTombstone;
        }

        @Override
        public String toString()
        {
            return "clustering: " + clustering + " data: " + (data == null ? "tombstone" : data) + ", state: " + state + ", timestamp: " + timestamp;
        }

        /**
         * Limiting to just the live CellStates in a given collection, returns a {@link ClusteringBounds} to constrain operations to that range.
         */
        public static ClusteringBounds getLiveClusteringBounds(Collection<CellState> cells)
        {
            if (cells.size() == 0)
                return ClusteringBounds.INVALID_CLUSTERING_BOUNDS;

            int minClustering = Integer.MAX_VALUE;
            int maxClustering = Integer.MIN_VALUE;

            for (CellState c : cells)
            {
                minClustering = Math.min(c.clustering, minClustering);
                maxClustering = Math.max(c.clustering, maxClustering);
            }

            return new ClusteringBounds(minClustering, ClusteringBoundType.INCLUSIVE, maxClustering, ClusteringBoundType.INCLUSIVE);
        }

        private static void cellTrace(String msg)
        {
            if (TRACE_CELL_STATE)
                logger.info(msg);
        }

        public static String toStringDifference(Map<Integer, CellState> expected, Map<Integer, CellState> seen, CellState expectedStatic, CellState seenStatic)
        {
            return "[Expected vs. Seen]" + '\n' +
                   "expectedStaticData:" + expectedStatic + '\n' +
                   "seenStaticData:" + seenStatic + '\n' +
                   "expectedLiveCells: " + liveCellCount(expected) + '\n' +
                   "seenLiveCells:     " + liveCellCount(seen) + '\n' +
                   "expectedCellTombstones: " + tombstonedCellCount(expected) + '\n' +
                   "seenCellTombstones:     " + tombstonedCellCount(seen) + '\n' +
                   "expectedRowTombstones: " + expected.values().stream().filter(CellState::isRowTombstoned).count() + '\n' +
                   "seenRowTombstones:    N/A";
        }
    }

    static int liveCellCount(Map<Integer, CellState> cells)
    {
        return (int) cells.values().stream().filter(CellState::isCellLive).count();
    }

    /**
     * We can't compare row or range tombstones in sets as those aren't sent back to the client
     */
    static int tombstonedCellCount(Map<Integer, CellState> cells)
    {
        return (int) cells.values().stream().filter(x -> x.state == LivenessState.CELL_TOMBSTONE).count();
    }

    public int expectedLiveCellCount()
    {
        return livenessCount(expectedCellState, LivenessState.LIVE);
    }

    public long expectedLiveRowCount()
    {
        return expectedCellState.values()
               .stream()
               .filter(CellState::isRowLive)
               .count();
    }

    /**
     * Calculates the number of live CELLS in the min INCLUSIVE max EXCLUSIVE range
     */
    public long liveCellsInRange(int minClustering, int maxClustering)
    {
        Preconditions.checkState(minClustering <= maxClustering);
        return expectedCellState.values()
               .stream()
               .filter(x -> x.isCellLive() && x.clustering >= minClustering && x.clustering < maxClustering)
               .count();
    }

    /**
     * Calculates the number of live ROWS in the min INCLUSIVE max EXCLUSIVE range
     */
    public long liveRowsInRange(int minClustering, int maxClustering)
    {
        Preconditions.checkState(minClustering <= maxClustering);
        return expectedCellState.values()
                                .stream()
                                .filter(x -> x.isRowLive() && x.clustering >= minClustering && x.clustering < maxClustering)
                                .count();
    }

    public List<Integer> getLiveClusterings()
    {
       return expectedCellState.values()
                               .stream()
                               .filter(CellState::isCellLive)
                               .map(x -> x.clustering)
                               .collect(Collectors.toList());
    }

    public List<Integer> getBoundedLiveClusterings(int minBoundInclusive, int maxBoundExclusive, Function<CellState, Boolean> filter)
    {
        return expectedCellState.values()
                                .stream()
                                .filter(filter::apply)
                                .map(x -> x.clustering)
                                .filter(c -> c >= minBoundInclusive && c < maxBoundExclusive)
                                .collect(Collectors.toList());
    }

    public int totalSeenLiveCells()
    {
        return livenessCount(seenCellState, LivenessState.LIVE);
    }

    public long totalSeenLiveRows()
    {
        return seenCellState.values().stream().filter(CellState::isRowLive).count();
    }

    public int totalExpectedCells()
    {
        return expectedCellState.size();
    }

    public int expectedTombstoneCellCount()
    {
        return (int) expectedCellState.values().stream().filter(x -> x.state != LivenessState.LIVE).count();
    }

    public int seenTombstoneCellCount()
    {
        return (int) seenCellState.values().stream().filter(x -> x.state != LivenessState.LIVE).count();
    }

    public void printExpectedCells()
    {
        logger.info("Printing cell state:");
        boolean inRT = false;
        for (CellState s : expectedCellState.values())
        {
            if (!s.isRangeTombstoned() && inRT)
            {
                logger.error("END RangeTombstone");
                inRT = false;
            }

            if (!inRT)
            {
                logger.error("   " + s);
            }

            if (!inRT && s.isRangeTombstoned())
            {
                logger.error("BEGIN Rangetombstone");
                inRT = true;
            }
        }
    }

    public void printExpectedCell(int clustering)
    {
        if (!expectedCellState.containsKey(clustering))
            logger.warn("Cannot print cell with no state in expected map: " + clustering);
        else
            logger.info("Expected state for cell with clustering: " + clustering + ": " + expectedCellState.get(clustering));
    }

    public void printExpectedLive(Predicate<? super CellState> check)
    {
        List<CellState> liveRows = expectedCellState.values().stream().filter(check).collect(Collectors.toList());
        if (liveRows.size() == 0)
            logger.info("No live rows found.");
        else
        {
            logger.info("Printing live rows:");
            for (CellState cs : liveRows)
                logger.info("Live row: " + cs);
        }
    }

    static int livenessCount(Map<Integer, CellState> toFilter, LivenessState state)
    {
        return (int) toFilter.values().stream().filter(x -> x.state == state).count();
    }

    void deleteRow(int clustering)
    {
        deleteRow(clustering, getNextTimestamp());
    }

    void deleteRow(int clustering, long timestamp)
    {
        session.execute(String.format("DELETE FROM %s USING TIMESTAMP %d WHERE key = 'key' AND clustering = %d;", TABLE, timestamp, clustering));
        Map<Integer, CellState> result = new HashMap<>();
        result.put(clustering, new CellState(clustering, COL + clustering, LivenessState.ROW_TOMBSTONE, timestamp));
        mergeExpected(result);
    }

    boolean generateStaticData()
    {
        return generateStaticData(getNextTimestamp());
    }

    boolean generateStaticData(long timestamp)
    {
        if (timestamp >= expectedStaticCell.timestamp)
        {
            String data = UUID.randomUUID().toString().substring(0, RandomHelpers.nextInt(36));
            session.execute(String.format("INSERT INTO %s (key, stat) VALUES ('key', '%s') USING TIMESTAMP %d;", TABLE, data, timestamp));
            traceExpected("Setting static data expectation to LIVE");
            expectedStaticCell = new CellState(STATIC_CLUSTERING, data, LivenessState.LIVE, timestamp);
            return true;
        }
        return false;
    }

    boolean deleteStaticData()
    {
        return deleteStaticData(getNextTimestamp());
    }

    boolean deleteStaticData(long timestamp)
    {
        if (timestamp >= expectedStaticCell.timestamp)
        {
            session.execute(String.format("DELETE stat FROM %s USING TIMESTAMP %d WHERE key = 'key';", TABLE, timestamp));
            traceExpected("Setting static data expectation to DELETED");
            expectedStaticCell = CellState.EMPTY_STATIC;
            return true;
        }
        return false;
    }

    void generateLinear(int offset, int count)
    {
        generateLinear(offset, count, getNextTimestamp());
    }

    /**
     * Generates both the requested amount of live data as well as a randomized string in the static column
     */
    void generateLinear(int offset, int count, long timestamp)
    {
        Map<Integer, CellState> insertedCells = new HashMap<>();
        for (int i = offset; i < offset + count; i++)
        {
            insertCQLRow(i, timestamp);
            insertedCells.put(i, new CellState(i, COL + i, LivenessState.LIVE, timestamp));
        }
        mergeExpected(insertedCells);
    }

    int generateRandomized(int targetCount, int minClustering, int maxClustering)
    {
        return generateRandomized(targetCount, minClustering, maxClustering, getNextTimestamp());
    }

    /**
     * Generates random live cells on the underlying table. Only creates live data on top of either deleted or non-existing
     * cells in the table; if something is already live this won't overlap that.
     *
     * @param targetCount Total number of unique entries to delete
     * @param maxClustering Max clustering value to generate for and delete
     * @param timestamp The timestamp to use for the deletion
     * @return count of cells added
     */
    int generateRandomized(int targetCount, int minClustering, int maxClustering, long timestamp)
    {
        Map<Integer, CellState> addedCells = new HashMap<>();

        int added = 0;

        // Create a list of all candidates between minClustering and maxClustering that aren't already live
        Set<Integer> existing = new HashSet<>(getLiveClusterings());
        List<Integer> candidates = IntStream.range(minClustering, maxClustering)
                                            .filter(x -> !existing.contains(x))
                                            .boxed()
                                            .collect(Collectors.toList());
        if (candidates.isEmpty())
        {
            logger.info("No candidates possible for generating new live data.");
            return 0;
        }

        if (targetCount > candidates.size())
        {
            logger.warn("Cannot add requested count of random new live cells: " + targetCount + ". Constraining to remaining available in range: " + candidates.size());
            targetCount = candidates.size();
        }
        RandomHelpers.shuffle(candidates);

        for (int i = 0; i < targetCount; i++)
        {
            int toAdd = candidates.get(i);
            insertCQLRow(toAdd, timestamp);
            addedCells.put(toAdd, new CellState(toAdd, String.valueOf(toAdd), LivenessState.LIVE, timestamp));
            ++added;
        }

        mergeExpected(addedCells);
        return added;
    }

    int deleteRandomCell(int count, int minClustering, int maxClustering)
    {
        return deleteRandomized(count, minClustering, maxClustering, LivenessState.CELL_TOMBSTONE, getNextTimestamp());
    }

    int deleteRandomRow(int count, int minClustering, int maxClustering)
    {
        return deleteRandomized(count, minClustering, maxClustering, LivenessState.ROW_TOMBSTONE, getNextTimestamp());
    }

    int deleteRandomized(int count, int minClustering, int maxClustering, LivenessState type)
    {
        return deleteRandomized(count, minClustering, maxClustering, type, getNextTimestamp());
    }

    /**
     * Generates random tombstones on the underlying table, either row or cell based on {@link LivenessState}. This will
     * avoid generating duplicate deletes on clusterings and instead spin until it fills out the total count requested.
     * It can delete less than requested depending on the available live cells / rows in the range requested.
     *
     * It will also only apply deletions to existing non-row-tombstoned cells.
     *
     * @param count Total number of unique entries to attempt to delete
     * @param minClustering Min clustering value inclusive to generate for and delete
     * @param maxClustering Max clustering value inclusive to generate for and delete
     * @param type Whether we're deleting rows or cells
     * @param timestamp The timestamp to use for the deletion
     * @return count of values actually deleted
     */
    int deleteRandomized(int count, int minClustering, int maxClustering, LivenessState type, long timestamp)
    {
        if (count <= 0)
            return 0;

        assert session != null;

        ClusteringBounds liveBounds = CellState.getLiveClusteringBounds(expectedCellState.values());

        // Adjust if we're trying to operate on a subset of this range
        final int filteredMinClustering = Math.max(minClustering, liveBounds.minClustering);
        final int filteredMaxClustering = Math.min(maxClustering, liveBounds.maxClustering);

        String isCell = type == LivenessState.CELL_TOMBSTONE ? " col " : "";
        Map<Integer, CellState> deletedCells = new HashMap<>();

        List<Integer> activeCellIndexes = getBoundedLiveClusterings(filteredMinClustering,
                                                                    filteredMaxClustering,
                                                                    x -> !x.isRowTombstoned() && !x.isRangeTombstoned());

        if (activeCellIndexes.size() == 0)
        {
            ClusteringBounds finalBounds = new ClusteringBounds(filteredMinClustering, ClusteringBoundType.INCLUSIVE, filteredMaxClustering, ClusteringBoundType.INCLUSIVE);
            trace("No active cells in the range provided (" + finalBounds + "); doing nothing.");
            return 0;
        }

        if (count >= activeCellIndexes.size())
        {
            logger.warn("Count: " + count + " is >= activeCellIndexes.size: " + activeCellIndexes.size() + ". Attempting to delete more cells than we have active; truncating to total live count");
            count = activeCellIndexes.size();
        }
        trace("activeCellIndexes count: " + activeCellIndexes);
        RandomHelpers.shuffle(activeCellIndexes);

        trace("Count that's live: " + expectedLiveCellCount());
        trace("count we're going to delete: " + count);
        for (int i = 0; i < count; i++)
        {
            int toDelete = activeCellIndexes.get(i);
            // ? String.format("DELETE %s FROM %s USING TIMESTAMP %d WHERE key = 'key' AND clustering = %d;", COL, TABLE, timestamp, idx)
            String command = String.format("DELETE %s FROM %s.%s USING TIMESTAMP %d WHERE key = 'key' AND clustering = %d;", isCell, KEYSPACE, TABLE, timestamp, toDelete);
            traceCQL(toDelete, command);
            session.execute(command);
            deletedCells.put(toDelete, new CellState(toDelete, null, type, timestamp));
        }
        trace("Min deletedCell idx: " + deletedCells.keySet().stream().min(Comparator.comparingInt(o -> o)));
        trace("Max deletedCell idx: " + deletedCells.keySet().stream().max(Comparator.comparingInt(o -> o)));
        mergeExpected(deletedCells);
        return deletedCells.size();
    }

    void deleteLinearCell(int startIndex, int count)
    {
        deleteLinear(startIndex, count, LivenessState.CELL_TOMBSTONE, getNextTimestamp());
    }

    void deleteLinearRow(int startIndex, int count)
    {
        deleteLinear(startIndex, count, LivenessState.ROW_TOMBSTONE, getNextTimestamp());
    }

    void deleteLinear(int startIndex, int count, LivenessState type)
    {
        deleteLinear(startIndex, count, type, getNextTimestamp());
    }

    void deleteLinear(int startIndex, int count, LivenessState type, long timestamp)
    {
        assert session != null;

        List<Integer> liveCells = getBoundedLiveClusterings(startIndex, startIndex + count, x -> !x.isRowTombstoned() && !x.isRangeTombstoned());
        trace("deleteLinear: startIdx: " + startIndex + ", count: " + count + ", found live cell size in range: " + liveCells.size());
        Map<Integer, CellState> deleted = new HashMap<>();
        for (int i = 0; i < liveCells.size(); i++)
        {
            int idx = liveCells.get(i);
            String query = type == LivenessState.CELL_TOMBSTONE
                           ? String.format("DELETE %s FROM %s USING TIMESTAMP %d WHERE key = 'key' AND clustering = %d;", COL, TABLE, timestamp, idx)
                           : String.format("DELETE FROM %s USING TIMESTAMP %d WHERE key = 'key' AND clustering = %d;", TABLE, timestamp, idx);
            traceCQL(idx, query);
            session.execute(query);
            deleted.put(idx, new CellState(idx, null, type, timestamp));
        }
        assertEquals(liveCells.size(), deleted.size());
        mergeExpected(deleted);
    }

    /**
     * Use when we're not particularly concerned about testing boundaries on clustering deletions and want to use the next default timestamp
     * @param minClustering min clustering INCLUSIVE
     * @param maxClustering max clustering EXCLUSIVE
     */
    void deleteRange(int minClustering, int maxClustering)
    {
        deleteRange(minClustering, maxClustering, getNextTimestamp());
    }

    /**
     * Use when we're not particularly concerned about testing boundaries on clustering deletions
     * @param minClustering min clustering INCLUSIVE
     * @param maxClustering max clustering EXCLUSIVE
     */
    void deleteRange(int minClustering, int maxClustering, long timestamp)
    {
        deleteRange(minClustering, ClusteringBoundType.INCLUSIVE, maxClustering, ClusteringBoundType.EXCLUSIVE, timestamp);
    }

    void deleteRange(ClusteringBounds bounds, long timestamp)
    {
        deleteRange(bounds.minClustering, bounds.minClusteringType, bounds.maxClustering, bounds.maxClusteringType, timestamp);
    }

    /**
     * Creates rt deletion markers for all clustering values between min and max, respecting bound type
     */
    void deleteRange(int minClustering, ClusteringBoundType minType, int maxClustering, ClusteringBoundType maxType, long timestamp)
    {
        Preconditions.checkState(minClustering <= maxClustering, String.format("Expect minClustering (%d) to be <= maxClustering (%d)", minClustering, maxClustering));
        if (minClustering == maxClustering)
            return;
        trace("Inside deleteRange. minClustering: " + minClustering + " and maxClustering: " + maxClustering);
        String minBound = minType == ClusteringBoundType.INCLUSIVE ? ">=" : ">";
        String maxBound = maxType == ClusteringBoundType.INCLUSIVE ? "<=" : "<";

        String query = "DELETE FROM %s USING TIMESTAMP " + timestamp + " WHERE key  = 'key' AND clustering " + minBound + minClustering + " AND clustering " + maxBound + maxClustering;
        trace("Executing query: " + query);
        executeTable(query);

        int minInclusiveBound = minType == ClusteringBoundType.EXCLUSIVE ? minClustering + 1 : minClustering;
        int maxInclusiveBound = maxType == ClusteringBoundType.EXCLUSIVE ? maxClustering - 1 : maxClustering;

        Map<Integer, CellState> deletedRows = new HashMap<>();
        ClusteringBounds rt = new ClusteringBounds(minInclusiveBound, ClusteringBoundType.INCLUSIVE, maxInclusiveBound, ClusteringBoundType.INCLUSIVE);
        for (int i = rt.minClustering; i <= rt.maxClustering; i++)
        {
            CellState cell = new CellState(i, null, LivenessState.RANGE_TOMBSTONE, timestamp);
            trace("Setting range tombstone on cell: " + cell.clustering);
            cell.setRangeTombstone(rt);
            if (deletedRows.put(i, cell) != null)
                throw new RuntimeException("Should never insert already seen clusterings here.");
        }
        trace("Size of deleted rows: " + deletedRows.size());
        mergeExpected(deletedRows);
    }

    public void executeTable(String command)
    {
        assert session != null;
        assert command != null;
        session.execute(String.format(command, TABLE));
    }

    public void executeKeyspace(String command)
    {
        session.execute(String.format(command, KEYSPACE));
    }

    public static void traceSeen(String msg)
    {
        if (TRACE_SEEN)
            trace(msg, TraceType.SEEN);
    }

    public static void traceExpected(String msg)
    {
        if (TRACE_EXPECTED)
            trace(msg, TraceType.EXPECTED);
    }

    public static void trace(String msg)
    {
        if (TRACE)
            trace(msg, TraceType.GENERAL);
    }

    public static void trace(String msg, TraceType type)
    {
        logger.info(type + "::" + msg);
    }

    public static void traceCQL(int clustering, String msg)
    {
        if ((TRACE_CLUSTERING != -1 && TRACE_CLUSTERING == clustering) || (TRACE_CLUSTERING == -1 && TRACE_CQL))
            logger.info(TraceType.CQL + "::" + msg);
    }

    /**
     * Because JDK8. /sigh
     * TODO: Can we remove this now that JDK8 support is dropped?
     */
    public static void appendLine(StringBuilder sb, String text)
    {
        sb.append(text).append("\n");
    }

    public long getNextTimestamp()
    {
        return nextTS++;
    }

    public void insertCQLRow(int clustering, long timestamp)
    {
        String command = "INSERT INTO %s (key, clustering, col) VALUES ('key', " + clustering + ", 'col" + clustering + "') USING TIMESTAMP " + timestamp + ';';
        traceCQL(clustering, command);
        executeTable(command);
    }

    public String toString()
    {
        return "Type: " + this.getClass() + '\n' +
               "pageSize: " + pageSize + '\n' +
               "failureThreshold: " + failureThreshold + '\n' +
               "reversed: " + reversed + "\n\n" +
               CellState.toStringDifference(expectedCellState, seenCellState, expectedStaticCell, seenStaticCell) + '\n';
    }

    public static void traceAll()
    {
        TRACE = true;
        TRACE_EXPECTED = true;
        TRACE_SEEN = true;
    }

    /**
     * Uses the "lazy fetch and fill automatically" approach to using the driver; we don't have the ability to introspect
     * on paging states using this paradigm, but we can reasonably expect some users out in the wild to be using the driver
     * with this pattern so want to confirm the Cell results returned match our model expectations.
     *
     * We have no intention of using this as a root class for an actual unit test; we instead want to access the mechanisms
     * inside {@link CQLTester} to setup a server and driver connection to exercise paging.
     */
    @SuppressWarnings("UnconstructableJUnitTestCase")
    public static class SyncTombstonePagingValidator extends AbstractPagingValidator
    {
        public SyncTombstonePagingValidator(int pageSize, int failureThreshold, boolean reversed)
        {
            super(pageSize, failureThreshold, reversed);
        }

        @Override
        public void queryData() throws ExecutionException, InterruptedException, TimeoutException
        {
            SimpleStatement statement = new SimpleStatement(String.format("SELECT * FROM %s WHERE key = 'key'", TABLE));
            statement.setFetchSize(pageSize);
            ResultSet rs = session.execute(statement);

            int firstClustering = Integer.MIN_VALUE;
            for (Row r : rs.all())
            {
                if (!r.isNull(STATIC_COL))
                {
                    traceSeen("Static cell is not null; adding as live seen.");
                    seenStaticCell = new CellState(STATIC_CLUSTERING, r.getString(STATIC_COL), LivenessState.LIVE, -1);
                }
                CellState state = CellState.fromRow(r);

                if (firstClustering == Integer.MIN_VALUE)
                    firstClustering = state.clustering;

                traceSeen("Adding cell: " + state);
                seenCellState.put(state.clustering, state);
            }
        }
    }

    /**
     * This tester's paradigm is to get the count of available rows, fetch them, update the count of items seen and paging
     * states as far as the client space understands (i.e. no row or range tombstones are visible), and step through the
     * paging state page by page. This is the paradigm we expect clients to use if they want to have more control over
     * when they continue to page in the face of a large number of tombstones.
     *
     * We have no intention of using this as a root class for an actual unit test; we instead want to access the mechanisms
     * inside {@link CQLTester} to setup a server and driver connection to exercise paging.
     */
    @SuppressWarnings("UnconstructableJUnitTestCase")
    public static class AsyncTombstonePagingValidator extends AbstractPagingValidator
    {
        public AsyncTombstonePagingValidator(int pageSize, int failureThreshold, boolean reversed)
        {
            super(pageSize, failureThreshold, reversed);
        }

        @Override
        public void queryData() throws ExecutionException, InterruptedException, TimeoutException
        {
            populateAsyncQueryData();
        }

        @Override
        public void prepareTest(int newPageSize, int failureThreshold)
        {
            super.prepareTest(newPageSize, failureThreshold);
        }

        @Override
        public void reset()
        {
            super.reset();
        }

        private void populateAsyncQueryData() throws ExecutionException, InterruptedException, TimeoutException
        {
            traceSeen("populating");
            SimpleStatement statement = generateQueryStatement();
            statement.setFetchSize(pageSize);
            ResultSet rs = session.executeAsync(statement).getUninterruptibly();

            if (rs.getExecutionInfo().getPagingState() == null)
                traceSeen("PAGING STATE NULL ON FIRST EXECUTION.");
            while (rs.getExecutionInfo().getPagingState() != null)
            {
                processResultSet(rs);
                rs = rs.fetchMoreResults().get(5, TimeUnit.SECONDS);
            }
            processResultSet(rs);
        }

        /**
         * For a given async fetched ResultSet, we'll process all the rows that are currently fetched without triggering
         * another page.
         */
        private void processResultSet(ResultSet rs)
        {
            int liveRows = rs.getAvailableWithoutFetching();

            int firstClustering = -1;
            boolean printedFirstKey = false;
            for (int i = 0; i < liveRows; i++)
            {
                Row r = rs.one();

                if (!printedFirstKey)
                    printedFirstKey = true;

                if (firstClustering == -1)
                    firstClustering = r.getInt(CLUSTERING);

                if (!r.isNull(STATIC_COL))
                {
                    traceSeen("Static cell is not null; adding live static.");
                    seenStaticCell = new CellState(STATIC_CLUSTERING, r.getString(STATIC_COL), LivenessState.LIVE, -1);
                }

                // Handle the case where the row is live due to static but clustering tombstoned
                if (r.getString(STATIC_COL) != null && r.isNull(CLUSTERING))
                {
                    traceSeen("Static cell is live but row is dead; decrementing live row count.");
                    liveRows--;
                }

                if (!r.isNull(CLUSTERING))
                {
                    // TODO: Consider whether to use writetime() to query out writetimes of data we see to help debug failures
                    CellState cell = CellState.fromRow(r);
                    seenCellState.put(cell.clustering, cell);
                }
            }
            traceSeen("LIVE processResults: firstClustering: " + firstClustering + ", liveRows: " + liveRows + ", pageSize: " + pageSize);
            traceSeen("seenLiveCells: " + liveCellCount(seenCellState) + ", seenTombstoneCells: " + tombstonedCellCount(seenCellState));
        }
    }
}