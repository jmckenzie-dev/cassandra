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
import java.text.MessageFormat;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;

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

    /** Debug method to show written files; useful when debugging specific tests. */
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

    /** Used during test debug to differentiate output visually */
    static void debugLog(String input)
    {
        logger.debug("\n\n****************   [TEST DEBUG]   *****************\n" +
                     input +
                     System.lineSeparator() +
                     "***************************************************\n\n" +
                     System.lineSeparator());
    }

    /**
     * Utility class that flags the replayer as having seen a CDC mutation and calculates offset but doesn't apply mutations.
     */
    static class MutationCountingReplayer extends CommitLogReplayer
    {
        final ConcurrentLinkedQueue<MutationIdentifier> seenMutations = new ConcurrentLinkedQueue<>();

        final ConcurrentHashMap<MutationIdentifier, Integer> duplicateMutations = new ConcurrentHashMap<>();
        final MutationCountingHandler mutationHandler = new MutationCountingHandler();

        MutationCountingReplayer() throws IOException
        {
            super(CommitLog.instance, CommitLogPosition.NONE, null, ReplayFilter.create());
            CommitLog.instance.sync(true);
        }

        void replayExistingCommitLog() throws IOException
        {
            for (File f: new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
            {
                commitLogReader.readCommitLogSegment(mutationHandler, f, true);
            }
        }

        void replaySingleFile(File f) throws IOException
        {
            commitLogReader.readCommitLogSegment(mutationHandler, f, true);
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

        private class MutationCountingHandler implements CommitLogReadHandler
        {
            public boolean shouldSkipSegmentOnError(CommitLogReadException exception)
            {
                return false;
            }

            public void handleUnrecoverableError(CommitLogReadException exception)
            {
                Assert.fail(MessageFormat.format("Got unrecoverable error during test: {0}", exception.getMessage()));
            }

            public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
            {
                MutationIdentifier id = new MutationIdentifier(m.getKeyspaceName(), m.key(), desc.id, size, entryLocation);

                if (m.trackedByCDC())
                    sawCDCMutation = true;

                if (seenMutations.contains(id))
                {
                    Integer pv = duplicateMutations.get(id);
                    if (pv == null)
                        pv = 0;
                    duplicateMutations.put(id, pv + 1);
                }
                else
                {
                    seenMutations.add(id);
                }
            }
        }

        public void reset()
        {
            seenMutations.clear();
            duplicateMutations.clear();
            sawCDCMutation = false;
        }

        /**
         * Helper class that allows us to uniquely identify a mutation at least within a single instance of a running node.
         */
        static class MutationIdentifier
        {
            final long segmentId;
            final int size;
            final int location;
            final String keyspaceName;
            final DecoratedKey decoratedKey;

            MutationIdentifier(String keyspaceName, DecoratedKey key, long segmentId, int size, int location)
            {
                this.keyspaceName = keyspaceName;
                this.decoratedKey = key;
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
                       other.segmentId == this.segmentId &&
                       other.keyspaceName.equals(this.keyspaceName) &&
                       other.decoratedKey.equals(this.decoratedKey);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(size, location, segmentId, keyspaceName, decoratedKey);
            }

            @Override
            public String toString()
            {
                return new StringBuilder()
                    .append("sId: ").append(segmentId).append(", ")
                    .append(" size: ").append(size).append(", ")
                    .append(" loc: ").append(location).append(", ")
                    .append(" ks: ").append(keyspaceName).append(", ")
                    .append(" dk: ").append(decoratedKey)
                    .toString();
            }
        }
    }

    static class NoopMutationHandler implements CommitLogReadHandler
    {
        public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
        {
            return false;
        }

        public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
        { }

        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        { }
    }
}
