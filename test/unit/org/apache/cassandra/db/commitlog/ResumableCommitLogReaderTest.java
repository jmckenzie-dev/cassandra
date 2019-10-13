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

package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

/* Forces CDC enabled on the run of these tests as they aren't used in non-CDC context and ResumableCommitLogReader
 * class asserts cdc enabled on creation.
 */
public class ResumableCommitLogReaderTest extends CQLTester
{
    private Random random = new Random();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setCDCEnabled(true);
        CQLTester.setUpClass();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();

        // Need to clean out any files from previous test runs. Prevents flaky test failures.
        CommitLog.instance.stopUnsafe(true);
        CommitLog.instance.start();

        // For each test, we start with the assumption of a populated set of a few files we can pull from.
        createTable("CREATE TABLE %s (a int, b int, c double, d decimal, e smallint, f tinyint, g blob, primary key (a, b))");

        byte[] bBlob = new byte[1024 * 1024];
        CommitLog.instance.sync(true);

        // Populate some CommitLog segments on disk
        for (int i = 0; i < 20; i++)
        {
            random.nextBytes(bBlob);

            logger.debug(String.format("Executing insert for index: [%d]", i));
            execute("INSERT INTO %s (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    random.nextInt(),
                    random.nextInt(),
                    random.nextDouble(),
                    random.nextLong(),
                    (short)random.nextInt(),
                    (byte)random.nextInt(),
                    ByteBuffer.wrap(bBlob));
        }
        CommitLog.instance.sync(true);

        // Should have well more than 3 segments to work with on subsequent tests.
        Assert.assertTrue(CommitLog.instance.segmentManager.getUnflushedSegments().size() > 3);
    }

    /**
     * Expect operation as though non-resumable, read file to end and complete.
     */
    @Test
    public void testNonResumedGeneralCase()
    {
        File writtenFile = CommitLogTestUtils.getFilledCommitLogFile();
        CommitLogTestUtils.debugLog(String.format("Got written CL segment: %s", writtenFile.toString()));
        CommitLogTestUtils.debugLog(String.format("Active segment writing to is: %s", CommitLog.instance.segmentManager.getActiveSegment().logFile.toString()));
        CommitLogTestUtils.listCommitLogFiles("Checking total CL files:");
    }

    /**
     * Expect operation to pick up and feed total mutation # consistent w/mutations in CL
     */
    @Test
    public void testSingleResumeCase()
    {
    }

    /**
     * Expect operation to pick up and feed total mutation # consistent w/mutations in CL, no errors.
     */
    @Test
    public void testResumeAtFileEndOffset()
    {
    }

    /**
     * If the offset provided by the user is past the end of the file itself, we expect an RTE
     */
    @Test
    public void testOffsetPastEnd() throws RuntimeException
    {
    }

    /**
     * If the offset provided by the user is < 0, we expect an RTE
     */
    @Test
    public void testNegativeOffset() throws RuntimeException
    {
    }

    /**
     * If the offset is at Zero, more or less expect no-op since the user didn't ask us to read anything. Worth allowing
     * this behavior in case offset / etc is written by someone to periodically poll and read to the CDC synced offset, so
     * 0 is a valid / expected behavior in that case.
     */
    @Test
    public void testOffsetAtZero()
    {
    }

    /**
     * Expect RTE if it's a corrupt input file.
     */
    @Test
    public void testCorruptInputFile()
    {
    }

    /**
     * Expect RTE if the input file is completely missing.
     */
    @Test
    public void testMissingInputFile()
    {
    }

    /**
     * Expect RTE if the input file exists and isn't a CLR. Overlaps w/"Corrupt", but want to make sure failure on header
     * parsing is handled as well as failure once header is validated but something blows up mid-file.
     */
    @Test
    public void testWrongFile()
    {
    }
}
