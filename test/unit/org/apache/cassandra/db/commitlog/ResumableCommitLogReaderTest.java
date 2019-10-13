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
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

/* Forces CDC enabled on the run of these tests as they aren't used in non-CDC context and ResumableCommitLogReader
 * class asserts cdc enabled on creation.
 *
 * Note: Many of these tests depend on having a CommitLog segment they're working on, and we don't force a stop of the
 * commit log segment allocator or flushing mechanisms while running these tests. As such, it's _possible_ we could end
 * up with files getting yanked out from under us and the test failing, but with 4+ segments created by the default ref
 * data population, the risk of that should be very low. Keep it in mind as time goes by.
 */
public class ResumableCommitLogReaderTest extends CQLTester
{

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

        // Start with a clean slate each test. Arguably could pre-populate and just use populated data; keep an eye
        // on runtime for this test suite and group if that becomes a worthy time and complexity tradeoff.
        CommitLog.instance.resetUnsafe(true);

        populateReferenceData(true);

        // Should have well more than 3 segments to work with on subsequent tests.
        Assert.assertTrue(CommitLog.instance.segmentManager.getSegmentsForUnflushedTables().size() > 3);
    }

    /**
     * Expect operation as though non-resumable, read file to end and complete.
     */
    @Test
    public void testNonResumedGeneralCase() throws IOException
    {
        File writtenFile = CommitLogTestUtils.getFilledCommitLogFile();
        Assert.assertTrue(writtenFile.length() > 0);

        CommitLogTestUtils.CDCMutationCountingReplayer testReplayer = new CommitLogTestUtils.CDCMutationCountingReplayer();
        testReplayer.replaySingleFile(writtenFile);

        Assert.assertTrue("Did not see any CDC enabled mutations.", testReplayer.sawCDCMutation);
        Assert.assertFalse("Saw a duplicate mutation while replaying a single file. This... shouldn't happen.",
                           testReplayer.hasSeenDuplicateMutations());
    }

    /**
     * Confirm our duplicate mutation testing infrastructure is working.
     */
    @Test
    public void testDuplicateCheckLogic() throws IOException
    {
        File writtenFile = CommitLogTestUtils.getFilledCommitLogFile();
        Assert.assertTrue(writtenFile.length() > 0);

        CommitLogTestUtils.CDCMutationCountingReplayer testReplayer = new CommitLogTestUtils.CDCMutationCountingReplayer();
        testReplayer.replaySingleFile(writtenFile);

        Assert.assertTrue("Did not see any CDC enabled mutations.", testReplayer.sawCDCMutation);
        Assert.assertFalse("Saw a duplicate mutation while replaying a single file. This... shouldn't happen.",
                           testReplayer.hasSeenDuplicateMutations());

        testReplayer.replaySingleFile(writtenFile);
        Assert.assertTrue("Expected to see duplicate mutations on 2nd replay of file.", testReplayer.hasSeenDuplicateMutations());
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
