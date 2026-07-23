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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.RandomHelpers;

import static org.apache.cassandra.service.pager.AbstractPagingValidator.AsyncTombstonePagingValidator;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.CellState;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.DEFAULT_FAILURE_THRESHOLD;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.LivenessState;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.liveCellCount;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.tombstonedCellCount;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.trace;
import static org.apache.cassandra.utils.RandomHelpers.nextInt;
import static org.apache.cassandra.utils.RandomHelpers.randomFromRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractPagingValidatorTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPagingValidatorTest.class);

    private static Session session;
    private static AsyncTombstonePagingValidator validator;
    private static final int ORIGINAL_PAGE_ACROSS = DatabaseDescriptor.getTombstonePagingThreshold();
    private static final int ORIGINAL_FAILURE_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    private static final int ORIGINAL_WARNING_THRESHOLD = DatabaseDescriptor.getTombstoneWarnThreshold();

    @AfterClass
    public static void afterClass()
    {
        session.close();
        DatabaseDescriptor.setTombstonePagingThreshold(ORIGINAL_PAGE_ACROSS);
        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_FAILURE_THRESHOLD);
        DatabaseDescriptor.setTombstoneWarnThreshold(ORIGINAL_WARNING_THRESHOLD);
    }

    @Before
    public void beforeTest()
    {
        if (session == null)
            session = sessionNet();
        if (validator == null)
        {
            validator = new AsyncTombstonePagingValidator(100, DEFAULT_FAILURE_THRESHOLD, false);
            validator.setSession(session);
        }
        validator.beforeTest();
    }

    @Test
    public void testStaticDataChecking()
    {
        for (int i = 0; i < 10; i++)
        {
            validator.generateStaticData();
            validator.deleteStaticData();
        }
        validator.generateStaticData();
        assertTrue(validator.staticDataExpected());
    }

    @Test
    public void testLinearGeneration()
    {
        int toGen = 100;
        validator.generateLinear(0, toGen);
        assertEquals(toGen, validator.expectedLiveCellCount());
        assertEquals(0, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testRandomGeneration()
    {
        int toGen = 100;
        validator.generateRandomized(toGen, 0, 500);

        assertEquals(toGen, validator.expectedLiveCellCount());
        assertEquals(0, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testRandomGenerationLimited()
    {
        int toGen = 100;
        validator.generateLinear(0, 75);
        int gen = validator.generateRandomized(100, 0, 100);
        assertEquals(25, gen);
        assertEquals(toGen, validator.expectedLiveCellCount());
        assertEquals(toGen, validator.expectedLiveRowCount());
        assertEquals(0, validator.expectedTombstoneCellCount());
    }

    /**
     * Randomized insertion is setup to keep spinning until we can ensure we inserted the expected number of rows.
     */
    @Test
    public void testNoOverlapInsertion()
    {
        validator.generateRandomized(25, 0, 50);
        validator.generateRandomized(25, 0, 50);
        assertEquals(50, validator.expectedLiveCellCount());
    }

    @Test
    public void testStaticDeletion()
    {
        for (int i = 0; i < 10; i++)
        {
            assertFalse(validator.staticDataExpected());
            validator.generateStaticData();
            assertTrue(validator.staticDataExpected());
            validator.deleteStaticData();
        }
    }

    @Test
    public void testLinearDeletion()
    {
        validator.generateLinear(0, 1000);
        assertEquals(1000, liveCellCount(validator.expectedCellState));

        // Confirm cell deletion expectation merges in
        validator.deleteLinear(25, 500, LivenessState.CELL_TOMBSTONE);
        assertEquals(500, liveCellCount(validator.expectedCellState));

        // Confirm row deletion expectation merges in
        validator.deleteLinear(525, 100, LivenessState.ROW_TOMBSTONE);
        assertEquals(400, liveCellCount(validator.expectedCellState));

        // Confirm overlap doesn't change anything
        validator.deleteLinear(525, 25, LivenessState.ROW_TOMBSTONE);
        assertEquals(400, liveCellCount(validator.expectedCellState));

        // Confirm we can insert back on top
        validator.generateLinear(0, 5000);
        assertEquals(5000, liveCellCount(validator.expectedCellState));
    }

    @Test
    public void testStaticAndNormalMix()
    {
        validator.generateStaticData();
        assertTrue(validator.staticDataExpected());
        validator.generateLinear(0, 500);
        assertEquals(500, liveCellCount(validator.expectedCellState));
        validator.deleteStaticData();
        assertEquals(500, liveCellCount(validator.expectedCellState));
        assertFalse(validator.staticDataExpected());
    }

    @Test
    public void testRandomDeletion()
    {
        validator.generateLinear(50, 1000);
        assertEquals(1000, liveCellCount(validator.expectedCellState));

        // Make sure the logic to not delete outside the live clustering range works
        validator.deleteRandomized(25, 40, 75, LivenessState.CELL_TOMBSTONE);
        assertEquals(25, tombstonedCellCount(validator.expectedCellState));
        assertEquals(975, liveCellCount(validator.expectedCellState));

        // Confirm that, if given a range outside what is live, we adjust and delete the expected amount
        validator.deleteRandomized(50, 500, 1500, LivenessState.ROW_TOMBSTONE);
        assertFalse(validator.staticDataExpected());

        // Since we deleted rows outside the range of the cell tombstones we can assert exact deletion counts
        assertEquals(75, validator.expectedTombstoneCellCount());
        assertEquals(925, validator.expectedLiveCellCount());
    }

    @Test
    public void testZero()
    {
        validator.generateLinear(0, 0);
        validator.generateRandomized(0, 0, 0);
        validator.deleteLinear(0, 0, LivenessState.CELL_TOMBSTONE);
        validator.deleteLinear(0, 0, LivenessState.ROW_TOMBSTONE);
        validator.deleteRandomized(0, 0, 0, LivenessState.CELL_TOMBSTONE);
    }

    @Test
    public void testReset()
    {
        validator.generateLinear(0, 1000);
        validator.deleteRandomized(500, 0, 1000, LivenessState.CELL_TOMBSTONE);
        assertEquals(500, validator.expectedTombstoneCellCount());
        assertEquals(500, validator.expectedLiveCellCount());
        assertEquals(1000, validator.expectedLiveRowCount());

        validator.reset();
        assertEquals(0, validator.expectedTombstoneCellCount());
        assertEquals(0, validator.expectedLiveCellCount());
        assertEquals(0, validator.expectedLiveRowCount());
    }

    @Test
    public void testTimestampReconciliation()
    {
        int ts = 10;
        validator.generateLinear(0, 1000, ts);

        // Confirm deletions at older time don't merge in
        validator.deleteLinear(0, 1000, LivenessState.CELL_TOMBSTONE, ts - 5);
        assertEquals(1000, validator.expectedLiveCellCount());

        validator.deleteRandomized(1000, 0, 1000, LivenessState.ROW_TOMBSTONE, ts - 5);
        assertEquals(1000, validator.expectedLiveRowCount());

        validator.deleteLinear(0, 1000, LivenessState.ROW_TOMBSTONE, ts + 5);
        assertEquals(0, validator.expectedLiveRowCount());
        assertEquals(0, validator.expectedLiveCellCount());
        assertEquals(1000, validator.expectedTombstoneCellCount());

        // Confirm insertions at older time don't merge in
        validator.generateLinear(0, 1000, ts);
        assertEquals(0, validator.expectedLiveRowCount());
        assertEquals(0, validator.expectedLiveCellCount());
        assertEquals(1000, validator.expectedTombstoneCellCount());

        validator.generateRandomized(250, 0, 1000, ts);
        assertEquals(0, validator.expectedLiveRowCount());
        assertEquals(0, validator.expectedLiveCellCount());
        assertEquals(1000, validator.expectedTombstoneCellCount());
    }

    /**
     * On randomized data addition and deletion, it needs to:
     * a) Pull from existing live cells, and
     * b) delete from that set without duplicates
     *
     * i.e. if you've randomly generated 100 cells and ask for 50 deletions, you should end up with 50 deleted cells and 50 live.
     */
    @Test
    public void testNoOverlapDeletionLogic()
    {
        validator.generateRandomized(100, 40, 1000);
        assertEquals(100, liveCellCount(validator.expectedCellState));

        validator.deleteRandomized(25, 0, 500, LivenessState.CELL_TOMBSTONE);
        assertEquals(25, tombstonedCellCount(validator.expectedCellState));
        assertEquals(75, validator.expectedLiveCellCount());

        validator.deleteRandomized(25, 500, 1233, LivenessState.ROW_TOMBSTONE);
        assertEquals(50, validator.expectedTombstoneCellCount());
        assertEquals(50, validator.expectedLiveCellCount());
    }

    /**
     * Confirm that requests for > the # requested available in a clustering range doesn't do anything unexpected
     */
    @Test
    public void testRandomizationConstraints()
    {
        validator.generateRandomized(100, 0, 50);
        assertEquals(50, validator.expectedLiveCellCount());

        validator.deleteRandomized(100, 0, 25, LivenessState.ROW_TOMBSTONE);
        validator.printExpectedCells();
        assertEquals(25, validator.expectedLiveCellCount());
        assertEquals(25, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testSimpleRangedTombstones()
    {
        validator.generateLinear(0, 100);
        assertEquals(100, validator.expectedLiveCellCount());

        validator.deleteRange(25, 110);
        assertEquals(25, validator.expectedLiveCellCount());
        assertFalse(validator.staticDataExpected());
        assertEquals(110, validator.totalExpectedCells());
        assertEquals(110 - 25, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testSparseLiveData()
    {
        for (int i = 0; i < 25; i++)
        {
            try
            {
                validator.reset();
                validator.prepareTest(100, DEFAULT_FAILURE_THRESHOLD);
                validator.generateLinear(0, 1000);
                validator.deleteLinear(0, 1000, LivenessState.ROW_TOMBSTONE);
                validator.generateRandomized(5, 0, 1000);
                assertEquals(5, validator.expectedLiveCellCount());

            }
            catch (AssertionError ae)
            {
                logger.error("Ran into a problem w/page size calculation w/sparse live data.");
                validator.printExpectedLive(CellState::isRowLive);
                throw ae;
            }
        }
    }

    @Test
    public void testOverlappingRangeTombstones()
    {
        validator.generateLinear(0, 1000);
        validator.deleteLinear(0, 100, LivenessState.ROW_TOMBSTONE);
        assertEquals(900, validator.expectedLiveRowCount());

        validator.deleteRange(0, 200);
        assertEquals(800, validator.expectedLiveRowCount());

        validator.deleteLinear(200, 100, LivenessState.CELL_TOMBSTONE);
        assertEquals(800, validator.expectedLiveRowCount());
        assertEquals(700, validator.expectedLiveCellCount());

        validator.deleteRange(400, 1000);
        assertEquals(200, validator.expectedLiveRowCount());
        assertEquals(100, validator.expectedLiveCellCount());
        assertEquals(900, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testRangeTombstoneSubset()
    {
        validator.generateLinear(0, 1000);
        validator.deleteRange(300, 400);
        validator.deleteRange(200, 500);
        assertEquals(700, validator.expectedLiveCellCount());
        assertEquals(300, validator.expectedTombstoneCellCount());
    }

    @Test
    public void testMixedTombstones()
    {
        validator.generateLinear(0, 10000);
        assertEquals(10000, validator.expectedLiveCellCount());

        validator.deleteRandomized(1500, 0, 30000, LivenessState.ROW_TOMBSTONE);
        validator.deleteRandomized(1500, 100, 25000, LivenessState.ROW_TOMBSTONE);
        assertEquals(7000, validator.expectedLiveCellCount());
        assertEquals(7000, validator.expectedLiveRowCount());
        assertEquals(3000, validator.expectedTombstoneCellCount());

        validator.deleteRange(-25, 15000);
        assertEquals(15025, validator.expectedTombstoneCellCount());
        assertEquals(0, validator.expectedLiveCellCount());
        assertEquals(0, validator.expectedLiveRowCount());
    }

    @Test
    public void testEmptyPageLogic()
    {
        validator.generateLinear(0, 1000);
        validator.deleteLinear(0, 980, LivenessState.ROW_TOMBSTONE);
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);
        assertEquals(20, validator.expectedLiveCellCount());
    }

    @Test
    public void testEmptyPageRangeTombstoneLogic()
    {
        validator.generateLinear(0, 1000);
        validator.deleteRange(0, 980);
        validator.prepareTest(200, DEFAULT_FAILURE_THRESHOLD);

        validator.generateLinear(0, 1000);
        validator.deleteRange(0, 200);
        validator.deleteLinear(200, 100, LivenessState.ROW_TOMBSTONE);
        assertEquals(700, validator.expectedLiveCellCount());
    }

    @Test
    public void fuzzTestModel()
    {
        RandomHelpers.printSeed("fuzzTestValidatorModel");
        // Generation goal and completed
        int genGoal = 0;
        int genComplete = 0;

        // Cell deletion goal and completed
        int cellDelGoal = 0;
        int cellDelComplete = 0;

        // Row deletion goal and completed
        int rowDelGoal = 0;
        int rowDelComplete = 0;

        int pass = 0;
        int max = 500;
        try
        {
            validator.deleteStaticData();
            // On each pass, we manually delete the range internally so each pass should generate "on top" of that flat state.
            while (pass < 100)
            {
                genGoal = randomFromRange(0, max);
                genComplete = validator.generateRandomized(genGoal, 0, max);

                // Nothing should get in the way of generating whatever we came up with here
                assertEquals(genGoal, genComplete);
                trace("genGoal: " + genGoal + ", genComplete: " + genComplete + " values between 0 and " + max);

                assertEquals(genComplete, validator.expectedLiveCellCount());
                assertEquals(genComplete, validator.expectedLiveRowCount());

                // Check that random deletion of rows on top of random gen data checks out
                if (validator.expectedLiveRowCount() > 0)
                {
                    rowDelGoal = nextInt(genComplete);
                    rowDelComplete = validator.deleteRandomized(rowDelGoal, 300, 500, LivenessState.ROW_TOMBSTONE);
                    trace("cellDelGoal: " + cellDelGoal + ", cellDelComplete : " + cellDelComplete + " in range 300 - 500.");
                    assertEquals(genComplete - rowDelComplete, validator.expectedLiveRowCount());
                    assertEquals(genComplete - rowDelComplete, validator.expectedLiveCellCount());
                }

                // Check that random deletion of cells on top of random gen data checks out
                if (validator.expectedLiveCellCount() > 0)
                {
                    cellDelGoal = nextInt(genComplete - rowDelComplete);
                    cellDelComplete = validator.deleteRandomized(cellDelGoal, 400, 600, LivenessState.CELL_TOMBSTONE);
                    trace("rowDelGoal: " + rowDelGoal + ", rowDelComplete: " + rowDelComplete + " in range 400-600.");
                    assertEquals(genComplete - rowDelComplete, validator.expectedLiveRowCount());
                    assertEquals(genComplete - (cellDelComplete + rowDelComplete), validator.expectedLiveCellCount());
                }

                // Flatten a range
                if (validator.expectedLiveRowCount() > 0)
                {
                    int minR = randomFromRange(0, max / 2);
                    int maxR = randomFromRange(minR, max);
                    long liveInRange = validator.liveRowsInRange(minR, maxR);
                    trace("minR: " + minR + " / maxR: " + maxR + ", liveInRange: " + liveInRange);

                    validator.deleteRange(minR, maxR);
                    assertEquals("Bad state coming out of range deletion.", genComplete - rowDelComplete - liveInRange, validator.expectedLiveRowCount());
                }

                if (pass % 2 == 0)
                    validator.deleteRange(0, max);
                else
                    validator.deleteLinear(0, max, LivenessState.ROW_TOMBSTONE);

                assertEquals(0, validator.expectedLiveRowCount());
                assertEquals(0, validator.expectedLiveCellCount());

                pass++;
            }
        }
        catch (AssertionError ae)
        {
            validator.printExpectedCells();
            StringBuilder sb = new StringBuilder()
                               .append("Invalid expected state during mixed validation fuzzing.")
                               .append("   pass: ").append(pass).append('\n')
                               .append("   toGen: ").append(genGoal).append('\n')
                               .append("   genC: ").append(genComplete).append('\n')
                               .append("   cdG: ").append(cellDelGoal).append('\n')
                               .append("   cdC: ").append(cellDelComplete).append('\n')
                               .append("   rdG: ").append(rowDelGoal).append('\n')
                               .append("   rdC: ").append(rowDelComplete).append('\n');
            logger.error(sb.toString());
            throw ae;
        }
    }
}