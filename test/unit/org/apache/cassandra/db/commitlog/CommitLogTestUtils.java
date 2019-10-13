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
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;

/**
 * Collection of some helper methods and classes for use in our various CommitLog Unit Tests
 */
class CommitLogTestUtils
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogTestUtils.class);

    static ByteBuffer randomizeBuffer(int size)
    {
        byte[] toWrap = new byte[size];
        new Random().nextBytes(toWrap);
        return ByteBuffer.wrap(toWrap);
    }

    static int getCDCRawCount()
    {
        return new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length;
    }

    static void expectCurrentCDCState(CommitLogSegment.CDCState expectedState)
    {
        CommitLogSegment.CDCState currentState = CommitLog.instance.segmentManager.getActiveSegment().getCDCState();
        if (currentState != expectedState)
        {
            logger.error("expectCurrentCDCState violation! Expected state: {}. Found state: {}. Current CDC allocation: {}",
                         expectedState, currentState, updateCDCTotalSize(CommitLog.instance.segmentManager));
            Assert.fail(String.format("Received unexpected CDCState on current active segment. Expected: %s. Received: %s",
                                      expectedState, currentState));
        }
    }

    static long updateCDCTotalSize(CommitLogSegmentManager segmentManager)
    {
        return ((CommitLogSegmentAllocatorCDC)segmentManager.segmentAllocator).updateCDCTotalSize();
    }

    /**
     * Pulls the back of the commit log files list and bails out if that is == our current allocating. Goal is to get a filled
     * and usable commit log segment for testing work.
     */
    static File getFilledCommitLogFile() throws NullPointerException
    {
        File result = new File(DatabaseDescriptor.getCommitLogLocation()).listFiles()[0];
        Assert.assertNotEquals(result.toString(), CommitLog.instance.segmentManager.getActiveSegment().logFile);
        return result;
    }

    /**
     * There's some possible raciness here, but for purposes of unit tests this should be deterministic enough not to cause
     * test failures, assuming we're doing deterministic writes when using this method.
     */
    static int getCommitLogCountOnDisk()
    {
        return CommitLog.instance.segmentManager.getSegmentsForUnflushedTables().size();
    }

    /**
     * For a given input file, writes randomized garbage from offset to end of file to "corrupt" it
     */
    static void corruptFileAtOffset(File f, int offset)
    {
    }

    /**
     * Debug method to allow printing of a message when sanity checking commit log state while writing tests
     */
    static void listCommitLogFiles(String message)
    {
        StringBuilder result = new StringBuilder();
        result.append(String.format("%s\n", message));
        result.append("List of files in CommitLog directory:\n");
        for (File f: new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
        {
            result.append(String.format("\t%s\n", f.getAbsolutePath()));
        }
        debugLog(result.toString());
    }

    /**
     * Used during test debug to differentiate output visually
     */
    static void debugLog(String input)
    {
        logger.debug("\n\n****************   [TEST DEBUG]   *****************\n" +
                     input +
                     System.lineSeparator() +
                     "***************************************************\n\n" +
                     System.lineSeparator());
    }

    /**
     * Utility class that flags the replayer as having seen a CDC mutation and calculates offset but doesn't apply mutations
     */
    static class CDCMutationCountingReplayer extends CommitLogReplayer
    {
        final Set<MutationIdentifier> seenMutations = Sets.newConcurrentHashSet();
        final ConcurrentHashMap<MutationIdentifier, Integer> duplicateMutations = new ConcurrentHashMap<>();

        CDCMutationCountingReplayer() throws IOException
        {
            super(CommitLog.instance, CommitLogPosition.NONE, null, ReplayFilter.create());
            CommitLog.instance.sync(true);
            commitLogReader = new CDCCountingReader();
        }

        /**
         * Takes existing files in the commit log location and forces a replay on them. Only really meaningful if you're
         * intercepting either the mutation read handler on replay or the mutation initiation object.
         */
        void replayExistingCommitLog() throws IOException
        {
            replayFiles(new File(DatabaseDescriptor.getCommitLogLocation()).listFiles());
        }

        void replaySingleFile(File f) throws IOException
        {
            replayFiles(new File[] { f });
        }

        boolean hasSeenMutation(MutationIdentifier id)
        {
            return seenMutations.contains(id);
        }

        int duplicateMutationCount(MutationIdentifier id)
        {
            Integer result = duplicateMutations.get(id);
            return result == null ? 0 : result;
        }

        boolean hasSeenDuplicateMutations()
        {
            return duplicateMutations.size() > 0;
        }

        private class CDCCountingReader extends CommitLogReader
        {
            @Override
            protected void readMutation(CommitLogReadHandler handler,
                                        byte[] inputBuffer,
                                        int size,
                                        CommitLogPosition minPosition,
                                        final int entryLocation,
                                        final CommitLogDescriptor desc)
            {
                MutationIdentifier id = new MutationIdentifier(minPosition.segmentId, size, entryLocation);

                RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size);
                Mutation mutation;
                try
                {
                    mutation = Mutation.serializer.deserialize(bufIn, desc.getMessagingVersion(), SerializationHelper.Flag.LOCAL);

                    if (mutation.trackedByCDC())
                    {
                        sawCDCMutation = true;
                        if (seenMutations.contains(id))
                        {
                            Integer pv = duplicateMutations.get(id);
                            if (pv == null)
                                pv = 0;
                            duplicateMutations.put(id, pv + 1);
                        }
                        seenMutations.add(id);
                    }
                }
                catch (IOException e)
                {
                    // Test fails.
                    throw new AssertionError(e);
                }
            }
        }

        /**
         * Helper class that allows us to uniquely identify a mutation at least within a single instance of a running node.
         */
        private static class MutationIdentifier
        {
            final long segmentId;
            final int size;
            final int location;

            MutationIdentifier(long segmentId, int size, int location)
            {
                this.segmentId = segmentId;
                this.size = size;
                this.location = location;
            }

            @Override
            public boolean equals(Object o)
            {
                if (o == this)
                    return true;

                if (!(o instanceof MutationIdentifier))
                    return false;

                MutationIdentifier other = (MutationIdentifier) o;

                return other.size == this.size &&
                       other.location == this.location &&
                       other.segmentId == this.segmentId;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(size, location, segmentId);
            }
        }
    }
}
