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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.RandomHelpers;

import static org.apache.cassandra.service.pager.AbstractPagingValidator.CellState;
import static org.apache.cassandra.service.pager.AbstractPagingValidator.appendLine;
import static org.apache.cassandra.utils.RandomHelpers.nextFloat;
import static org.apache.cassandra.utils.RandomHelpers.nextInt;
import static org.apache.cassandra.utils.RandomHelpers.nextLong;
import static org.apache.cassandra.utils.RandomHelpers.randomFromRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PagingFuzzTest extends CQLTester
{
    private static boolean TRACE = false;

    // Per-parameter fuzz duration. Both @Parameterized profiles (Sync + Async) run in one forked JVM under
    // ant's test.long.timeout (600s), so (numProfiles * duration) + startup must stay under it. At 2 profiles,
    // 3 min each (~6 min + overhead) fits with margin; override with -Dcassandra.test.paging_fuzz_duration_min.
    private static final int TEST_DURATION_MIN = Integer.getInteger("cassandra.test.paging_fuzz_duration_min", 3);

    protected static final int UNINIT = Integer.MIN_VALUE;

    private static final Logger logger = LoggerFactory.getLogger(PagingFuzzTest.class);

    private final AbstractPagingValidator validator;
    private Session session;

    // Parameterized runner invokes the static @Parameters method (forcing class init) before @BeforeClass runs,
    // so DatabaseDescriptor.conf is null unless we initialize it here, ahead of the ORIGINAL_* field reads below.
    static { DatabaseDescriptor.daemonInitialization(); }

    private static final int ORIGINAL_PAGE_ACROSS = DatabaseDescriptor.getTombstonePagingThreshold();
    private static final int ORIGINAL_FAILURE_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
    private static final int ORIGINAL_WARNING_THRESHOLD = DatabaseDescriptor.getTombstoneWarnThreshold();

    private static final String SYNC_TEST = "SyncPageTombstones";
    private static final String ASYNC_TEST = "AsyncPageTombstones";

    private static final int DATA_CEILING_MAX = 1000;

    private int iterations = 0;
    private int insertions = 0;
    private int deletions = 0;
    private int rangeDeletes = 0;
    private int reversals = 0;
    private int forwards = 0;
    private int statics = 0;

    /*
     TODO: Determine if there's a more idiomatic way to make the test case translation robust in the face of change
     Consider whether we even need durability here. If we have a SHA of a test and know the history, we can recreate the
     failure. So perhaps we just... don't bother with the translation? And assign integer values to each enum record but
     keep them split into different enums? Or just have them all as one enum and weight the results based on that hard-coded?

     The way we have it now helps abstract away the mapping of float -> integer value for enum, but it's not so laborious as
     all that compared to carrying along this somewhat complex (though not terrible I suppose) translation logic.
     */

    private interface PagingFuzzCommand
    {
        String getName();
    }

    private enum GenerateCommand implements PagingFuzzCommand
    {
        GENERATE_LINEAR("generateLinear"),
        GENERATE_RANDOM("generateRandomized");

        private final String name;
        GenerateCommand(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    private enum DeleteCommand implements PagingFuzzCommand
    {
        DELETE_LINEAR_CELL("deleteLinearCell"),
        DELETE_LINEAR_ROW("deleteLinearRow"),
        DELETE_RANGE("deleteRange"),
        DELETE_RANDOM_CELL("deleteRandomCell"),
        DELETE_RANDOM_ROW("deleteRandomRow");

        private final String name;
        DeleteCommand(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    private enum StateCommand implements PagingFuzzCommand
    {
        TOGGLE_REVERSE("toggleReversed"),
        TOGGLE_STATIC("toggleStatic");

        private final String name;
        StateCommand(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> pagers()
    {
        return Arrays.asList(new Object[][]{
            { SYNC_TEST, new AbstractPagingValidator.SyncTombstonePagingValidator(UNINIT, UNINIT, false) },
            { ASYNC_TEST, new AbstractPagingValidator.AsyncTombstonePagingValidator(UNINIT, UNINIT, false) }
            });
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

    public PagingFuzzTest(String name, AbstractPagingValidator validator)
    {
        this.validator = validator;
    }

    @Test
    public void testFuzzPagingAcrossTombstones() throws ExecutionException, InterruptedException, TimeoutException
    {
        boolean isRepro = RandomHelpers.maybeSetToUserSeed();
        long runSeed = RandomHelpers.getSeed();

        // TODO: Consider making the runtime configurable via env var
        long end = Clock.Global.currentTimeMillis() + TimeUnit.MINUTES.toMillis(TEST_DURATION_MIN);
        DatabaseDescriptor.setTombstonePagingThreshold(25000);

        List<String> reproCommandHistory = new ArrayList<>();

        while (Clock.Global.currentTimeMillis() < end)
        {
            if (isRepro)
                end = Long.MIN_VALUE;

            try
            {
                // Keep our ordering in this context so we can print out the chain of events that led to a failure
                validator.reset();
                randomizeForNextTest();
                reproCommandHistory = populateData();
                validator.runTest();

                // While this in no way adds any useful new randomization, it lets us snapshot the seed between tests so,
                // in the event of a failure, we can re-run with just the one test that failed rather than having to re-execute
                // the chain to get here.
                runSeed = nextLong();
                RandomHelpers.setSeed(runSeed);
            }
            catch (AssertionError ae)
            {
                logger.error("Error found during run! Seed: " + RandomHelpers.getSeed());
                logger.error("State of validator: " + validator);

                logger.error("Seed: " + runSeed);

                logger.error("\n\n[STEPS TO REPRODUCE]\nTo reproduce this test run, re-run the test with the 'cassandra.test.seed' env var set: [cassandra.test.env=" + runSeed + "]\n" +
                             "Alternatively, copy and paste the following as the first line int the test fuzzer's main method: \n" +
                             "   RandomHelpers.setUserSeed(" + runSeed + "L);\n\n");

                StringBuilder sb = new StringBuilder();
                appendLine(sb, "----------------- ");
                appendLine(sb, "Alternatively, copy and paste the following unit test into a test file and run it:");
                appendLine(sb, "[UNIT TEST TO REPRODUCE]");
                appendLine(sb, "@Test");
                sb.append("public void repro_").append(String.valueOf(runSeed).replace("-", "N")).append("() throws ExecutionException, InterruptedException, TimeoutException\n");
                appendLine(sb, "{");
                sb.append("    RandomHelpers.setSeed(").append(runSeed).append("L);\n");
                appendLine(sb, "    validator.pageSize = " + validator.pageSize + ";");
                appendLine(sb, "    validator.failureThreshold = " + validator.failureThreshold + ";");
                for (String cmd : reproCommandHistory)
                    appendLine(sb, "    " + cmd);
                appendLine(sb, "    validator.runTest();");
                appendLine(sb, "}");
                logger.error(sb.toString());
                throw ae;
            }
            finally
            {
                // error and info are interleaving
                Thread.sleep(250);
                logger.info("----------------" + '\n' +
                            "[PROGRESS]: " + '\n' +
                            "iterations: " + iterations + '\n' +
                            "insertions: " + insertions + '\n' +
                            "deletions: " + deletions + '\n' +
                            "rangeDeletes: " + rangeDeletes + '\n' +
                            "forward: " + forwards + '\n' +
                            "reversals: " + reversals + '\n' +
                            "statics: " + statics + '\n');
            }
        }
    }

    private List<PagingFuzzCommand> generateCommands()
    {
        List<PagingFuzzCommand> commandList = new ArrayList<>();

        // We want to cover a wide variety of weightings between addition and deletions to exercise all extremes of the
        // paging logic based on ratios of tombstones vs. live data.
        int targetCommands = randomFromRange(10, 50);
        float addWeight = nextFloat();

        // Execute either a reverse or a static state change about 10% of the time
        float commandWeight = .1f;

        while (commandList.size() < targetCommands)
        {
            float nextCommand = nextFloat();
            if (nextCommand <= addWeight)
                commandList.add(GenerateCommand.values()[nextInt(GenerateCommand.values().length)]);
            else
                commandList.add(DeleteCommand.values()[nextInt(DeleteCommand.values().length)]);

            if (commandList.size() == targetCommands)
                break;

            // Keep the ratio / pct generation for state commands separate from add/remove. That way we always either add
            // or remove some data on each pass.
            if (nextCommand <= commandWeight)
                commandList.add(StateCommand.values()[nextInt(StateCommand.values().length)]);
        }
        assertEquals(targetCommands, commandList.size());
        return commandList;
    }

    /**
     * Generates both a string representation of the command history and triggers the generation on the validator
     * @return The list of commands run on the test for reproduction if needed
     */
    private List<String> populateData()
    {
        List<PagingFuzzCommand> commands = generateCommands();
        List<String> reproCommands = new ArrayList<>();

        assertTrue(commands.size() > 0);

        for (PagingFuzzCommand cmd : commands)
        {
            int s = randomFromRange(0, DATA_CEILING_MAX - 100);
            int e = randomFromRange(s + 1, DATA_CEILING_MAX);
            int r = nextInt(e - s);
            if (r == 0)
                r = 1;

            String command = cmd.getName();
            switch (command)
            {
                case "generateLinear":
                    validator.generateLinear(s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    insertions += (e - s);
                    break;
                case "generateRandomized":
                    validator.generateRandomized(r, s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    insertions += r;
                    break;
                case "deleteLinearCell":
                    validator.deleteLinearCell(s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    deletions += (e - s);
                    break;
                case "deleteLinearRow":
                    validator.deleteLinearRow(s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    deletions += (e - s);
                    break;
                case "deleteRange":
                    validator.deleteRange(s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    ++rangeDeletes;
                    break;
                case "deleteRandomCell":
                    validator.deleteRandomCell(r, s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    deletions += r;
                    break;
                case "deleteRandomRow":
                    validator.deleteRandomRow(r, s, e);
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    deletions += r;
                    break;
                case "toggleReversed":
                    validator.toggleReversed();
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    break;
                case "toggleStatic":
                    validator.toggleStatic();
                    reproCommands.add(buildReproCommand(command, s, e, r));
                    break;
                default:
                    throw new IllegalArgumentException("Got unknown command: " + cmd);
            }
        }
        if (validator.reversed)
            ++reversals;
        else
            ++forwards;

        if (validator.staticDataExpected())
            ++statics;

        ++iterations;
        return reproCommands;
    }

    private String buildReproCommand(String commandName, int start, int end, int count)
    {
        trace(commandName + "::" + "s:" + start + ", e: " + end + ", c: " + count);
        if (commandName.contains("Random"))
            return "validator." + commandName + "(" + count + ", " + start + ", " + end + ");";
        else if (commandName.contains("toggle"))
            return "validator." + commandName + "();";
        else
            return "validator." + commandName + "(" + start + ", " + end + ");";
    }

    /**
     * Notably also clears out the seen {@link CellState} in our validator during {@link AbstractPagingValidator#prepareTest}
     */
    private void randomizeForNextTest()
    {
        int pageSize = randomFromRange(1, 10000);
        int failureThreshold = randomFromRange(1, 10000);
        trace("Preparing validator with pageSize: " + pageSize + " and failureThreshold: " + failureThreshold);
        validator.beforeTest();
        validator.prepareTest(pageSize, failureThreshold);
    }

    private static void trace(String msg)
    {
        if (TRACE)
            logger.info("PagingFuzz::" + msg);
    }
}