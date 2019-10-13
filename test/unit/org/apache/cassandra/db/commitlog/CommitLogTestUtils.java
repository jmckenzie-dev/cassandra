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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;

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

    static File getFilledCommitLogFile()
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
}
