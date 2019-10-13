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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

/**
 * Tests various alignments, offsets, and operations of the {@link ResumableCommitLogReader}
 */
public class ResumableCommitLogReaderTest extends CQLTester
{
    private CommitLogSegment populatedSegment;
    private CommitLogTestUtils.MutationCountingReplayer testReplayer;

    @Before
    public void setUpTest() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        testReplayer = new CommitLogTestUtils.MutationCountingReplayer();
        populateReferenceData(true);

        // Should have well more than 3 segments to work with on subsequent tests.
        Assert.assertTrue(CommitLog.instance.segmentManager.getSegmentsForUnflushedTables().size() > 3);

        // And always reset which file we're using as including CDC Mutations, since things may change between tests
        CommitLogTestUtils.MutationCountingReplayer testReplayer = new CommitLogTestUtils.MutationCountingReplayer();
        for (CommitLogSegment cls : CommitLog.instance.segmentManager.getSegmentsForUnflushedTables())
        {
            testReplayer.replaySingleFile(cls.logFile);
            if (testReplayer.sawCDCMutation)
            {
                populatedSegment = cls;
                return;
            }
        }
        throw new RuntimeException("No mutations seen in passed in collection.");
    }

    /**
     * Expect operation as though non-resumable, read file to end and complete.
     */
    @Test
    public void testNonResumedGeneralCase() throws Throwable
    {
        testReplayer.replaySingleFile(populatedSegment.logFile);

        Assert.assertTrue("Did not see any CDC enabled mutations.", testReplayer.sawCDCMutation);
        Assert.assertFalse("Saw a duplicate mutation while replaying a single file. This... shouldn't happen.",
                           testReplayer.hasSeenDuplicateMutations());
    }

    /**
     * Confirm our duplicate mutation testing infrastructure is working.
     */
    @Test
    public void testDuplicateCheckLogic() throws Throwable
    {
        testReplayer.replaySingleFile(populatedSegment.logFile);

        Assert.assertTrue("Did not see any CDC enabled mutations.", testReplayer.sawCDCMutation);
        Assert.assertFalse("Saw a duplicate mutation while replaying a single file. This... shouldn't happen.",
                           testReplayer.hasSeenDuplicateMutations());

        testReplayer.replaySingleFile(populatedSegment.logFile);
        Assert.assertTrue("Expected to see duplicate mutations on 2nd replay of file.", testReplayer.hasSeenDuplicateMutations());
    }

    /**
     * Expect operation to pick up where left off and feed total mutation # consistent w/mutations in CL, so we do a
     * single normal full replay, then a 2 step replay to ensure the total mutation count is as expected.
     */
    @Test
    public void testSingleResumeCase() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);
        Assert.assertTrue("Failed to successfully perform a start to finish CL read.", expectedCount != 0);

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            // This runs the risk of being flaky, since if we don't have CDC mutations in the first or second half,
            // this test will fail out. This has been 100% stable at the .5 barrier both non and compressed, but keep in mind.
            rr.readPartial((int)(rr.rawReader.length() * .5));
            Assert.assertFalse("Resumable Reader got constructed badly somehow.", rr.isClosed);
            Assert.assertNotNull("Resumable Reader doesn't have a RAR cached in it as expected.", rr.rawReader);

            // Confirm we didn't just parse everything in the first part
            Assert.assertNotEquals(expectedCount, testReplayer.seenMutations.size());

            int interimCount = testReplayer.seenMutations.size();
            Assert.assertTrue("Failed on initial partial replay", interimCount != 0);

            Assert.assertFalse("Expected reader to be open still.", rr.isClosed());
            Assert.assertTrue("Interim replay should have played back less than a full replay. Check logs.", interimCount < expectedCount);

            rr.readPartial(CommitLogReader.READ_TO_END_OF_FILE);
            Assert.assertEquals("Expected resumable read to give same # mutations as non but did not.",
                                testReplayer.seenMutations.size(),
                                expectedCount);
        }
    }

    /** Test multiple resumes w/end not matching SyncSegment offsets. */
    @Test
    public void testMultipleResumeNonAligned() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial(1024);

            // Sentinel to keep from locking the test. Since we are misusing the API by not sending the end sentinel,
            // this checks to see if that kind of "bad offset overflow" gives us both a) a stable API, and b) the right
            // parsed results from our end file.
            int limit = 50;

            // throw some strange offsets at this and make sure it's robust to them, non-multiple of SyncSegment end
            int offset = 1024 * 512;
            while (limit > 0 && !rr.isClosed() && !rr.readToExhaustion)
            {
                rr.readPartial(offset);
                offset += 1024 * 512;
                --limit;
            }
            rr.readToCompletion();
        }
        Assert.assertEquals("Expected non-aligned resumable read to give same # mutations as non but did not.",
                            expectedCount,
                            testReplayer.seenMutations.size());
    }

    /** Ensure that resumable readers w/offsets at sync segment boundaries don't blow up logic */
    @Test
    public void testResumingAtAlignedOffsets() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);

        ArrayList<Integer> segmentBoundaries = new ArrayList<>();
        // First, we want to get a list of all the segment boundaries out of the SegmentIterator
        try(ResumableCommitLogReader fr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            fr.offsetLimit = CommitLogReader.READ_TO_END_OF_FILE;
            CommitLogSegmentReader lsr = new CommitLogSegmentReader(fr);
            for (CommitLogSegmentReader.SyncSegment ss : lsr)
            {
                segmentBoundaries.add(ss.endPosition);
            }
            Assert.assertTrue(segmentBoundaries.size() > 0);
        }
        Assert.assertNotEquals(-1, expectedCount);
        testReplayer.reset();

        // And now we iterate through the file using those boundaries, ensuring it works in our resumable reader.
        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            // Confirm first read doesn't exhaust so the test is actually testing something.
            rr.readPartial(segmentBoundaries.get(0));
            Assert.assertFalse(rr.readToExhaustion);
            Assert.assertFalse(rr.isClosed);

            for (Integer offset : segmentBoundaries)
            {
                rr.readPartial(offset);
            }
            rr.readToCompletion();

            Assert.assertEquals("Reading based on segment offsets produced unexpected results.",
                                expectedCount,
                                testReplayer.seenMutations.size());
        }
    }

    /** Expect operation to pick up and feed total mutation # consistent w/mutations in CL, no errors. */
    @Test
    public void testResumeAtFileEndOffset() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial((int) populatedSegment.logFile.length());
            Assert.assertEquals(expectedCount, testReplayer.seenMutations.size());
        }
    }

    /**
     * If the offset provided by the user is past the end of the file itself, we expect a graceful read when using the logic
     * on a resumable reader as we read to end of file.
     */
    @Test
    public void testOffsetPastEnd() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial(Integer.MAX_VALUE);
            Assert.assertEquals(expectedCount, testReplayer.seenMutations.size());
        }
    }

    /** If the offset provided by the user is < 0, we expect a no-op on read followed by the ability to resume and read */
    @Test
    public void testNegativeOffset() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial(-1);
            // Expect nothing to have been read so at beginning of file still
            Assert.assertFalse(rr.readToExhaustion);
            Assert.assertEquals(0, testReplayer.seenMutations.size());

            // Then re-use the infra to read to end
            rr.readPartial(CommitLogReader.READ_TO_END_OF_FILE);
            Assert.assertEquals(expectedCount, testReplayer.seenMutations.size());
            Assert.assertEquals(0, testReplayer.duplicateMutations.size());
        }
    }

    /**
     * Confirm we gracefully handle cases where people may put in an offset that regresses what we're reading. Since the
     * RAR and iteration should be unidirectional, we should still not see duplicates.
     */
    @Test
    public void testRepeatOffsets() throws Throwable
    {
        int expectedCount = getExpectedMutationCount(populatedSegment.logFile);
        Assert.assertTrue(testReplayer.duplicateMutations.isEmpty());
        Assert.assertNotEquals(0, expectedCount);

        // Cache what we saw on first replay vs. newest, confirm >= all original seen
        ArrayList ids = new ArrayList<>(testReplayer.seenMutations);
        testReplayer.reset();

        ArrayList newIds = null;

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(populatedSegment.logFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial(1024 * 512);
            int countBeforeRegression = testReplayer.seenMutations.size();

            // Confirm no reads if we regress our offset
            rr.readPartial(1024 * 256);
            Assert.assertEquals(0, testReplayer.duplicateMutations.size());
            Assert.assertEquals(countBeforeRegression, testReplayer.seenMutations.size());

            // Confirm can resume w/correct offset
            rr.readPartial(CommitLogReader.READ_TO_END_OF_FILE);

            newIds = new ArrayList<>(testReplayer.seenMutations);

            // Confirm all mutation id's seen in straight read are seen by partial.
            for (Object id : ids)
                if (!newIds.contains(id))
                    Assert.fail(String.format("Missing id in resumable replay: %s", id));
            Assert.assertTrue(testReplayer.duplicateMutations.size() > 0);
        }
    }

    /**
     * Expect RTE if the input file is completely missing. Since these exceptions are user-facing in CL consumption in
     * a CDC context, this unit test serves to calcify that UI interaction a bit and confirm we're deliberate about changing
     * the type of exception we're throwing.
     */
    @Test
    public void testMissingInputFile() throws Throwable
    {
        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(new File("This_should_fail.txt"),
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        { }
        catch (RuntimeException e)
        {
            if (!e.getMessage().contains("version"))
                Assert.fail();
            return;
        }
        Assert.fail("Expected RuntimeException on creation. Did not get it.");
    }

    @Test
    public void testWrongFile() throws Throwable
    {
        File tempFile = File.createTempFile("test_file", ".tmp");
        try(FileWriter writer = new FileWriter(tempFile))
        {
            writer.write("This is really not a commit log header. This will go badly.");
        }

        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(tempFile,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        { }
        catch (RuntimeException rte)
        {
            if (rte.getMessage().contains("version"))
                return;
        }
        Assert.fail("Expected RuntimeException complaining about inability to parse version from CommitLogHeader");
    }

    /**
     * Uses CDC pipeline to read CDC index file + hard linked CDC File w/multiple mutations to confirm user use-case is functional.
     * This test requires CDC to be enabled to run as we need the CDC Allocator to be hardlinking files, etc.
     *
     * This test is unique (and has surfaced multiple pain points) in that it is the only test that is processing
     * a file being actively written.
     */
    @Test
    public void testUsingCDCOffsets() throws Throwable
    {
        if (!(CommitLog.instance.segmentManager.segmentAllocator instanceof CommitLogSegmentAllocatorCDC))
            return;

        CommitLogSegment activeSegment = CommitLog.instance.segmentManager.getActiveSegment();
        Assert.assertSame(activeSegment.getCDCState(), CommitLogSegment.CDCState.CONTAINS);

        File cdcSegment = activeSegment.getCDCFile();
        Assert.assertTrue(cdcSegment.exists());

        // Confirm we have a reasonable offset now and can scan to it
        int cdcOffset = parseCDCOffset(activeSegment.getCDCIndexFile());
        Assert.assertNotEquals(0, cdcOffset);

        int totalReadThroughCDC = -1;
        // Start a resumable reader wrapped around our active segment
        try(ResumableCommitLogReader rr = new ResumableCommitLogReader(cdcSegment,
                                                                       testReplayer.mutationHandler,
                                                                       CommitLogPosition.NONE,
                                                                       CommitLogReader.ALL_MUTATIONS,
                                                                       true))
        {
            rr.readPartial(cdcOffset);
            int lastMutationsRead = testReplayer.seenMutations.size();
            Assert.assertNotEquals(0, lastMutationsRead);

            byte[] buffer = new byte[1024 * 128];

            int tries = 250;
            int newCDCOffset;
            while (tries > 0)
            {
                cdcOffset = parseCDCOffset(activeSegment.getCDCIndexFile());
                writeReferenceLines(50, buffer);
                CommitLog.instance.sync(true);

                newCDCOffset = parseCDCOffset(activeSegment.getCDCIndexFile());
                // Look for 1 cdcOffset change and read it. We stop here since we can't really deterministically get
                // a repeatable test on # of writes leading to # of SyncSegment's written w/compression, etc.
                if (cdcOffset != newCDCOffset)
                {
                    rr.readPartial(newCDCOffset);
                    break;
                }
                tries--;
            }
            Assert.assertNotEquals(0, tries);
            // Confirm we read far enough to exhaust the underlying buffer in compression context. This will exercise the RAR re-alloc code.
            if (DatabaseDescriptor.getCommitLogCompression() != null)
                Assert.assertTrue(rr.readToExhaustion);

            // Write a chunk more data; hopefully cycles it so we have a full file, though not particularly relevant to our needs.
            buffer = new byte[1024 * 512];
            writeReferenceLines(150, buffer);
            CommitLog.instance.sync(true);

            rr.readToCompletion();
            totalReadThroughCDC = testReplayer.seenMutations.size();
        }

        // Get final count with one straight through read, confirm matches CDC staged
        testReplayer.reset();
        int expectedSeen = getExpectedMutationCount(cdcSegment);
        Assert.assertEquals(expectedSeen, totalReadThroughCDC);
    }

    /** Straightforward single non-resumable replay to count mutations expected. Assumes you're looking for > 0 */
    private int getExpectedMutationCount(File file) throws IOException
    {
        testReplayer.replaySingleFile(file);
        int expectedCount = testReplayer.seenMutations.size();
        testReplayer.reset();
        Assert.assertTrue("Failed to successfully perform a start to finish CL read.", expectedCount != 0);
        return expectedCount;
    }

    private int parseCDCOffset(File cdcIndexFile) throws IOException
    {
        try(BufferedReader br = new BufferedReader(new FileReader(cdcIndexFile)))
        {
            return Integer.parseInt(br.readLine());
        }
    }
}
