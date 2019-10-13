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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Pair;

/**
 * A state holder for a potentially resumable read. As we want to resume our reading with existing file pointers, buffers,
 * and sentinels without re-opening a file and re-decompressing or decrypting, we store references to a {@link CommitLogSegmentReader},
 * to a {@link RandomAccessReader}, and to an iterator of {@link CommitLogSegmentReader.SyncSegment} here for re-use.
 *
 * This serves dual purpose as an API endpoint and logical state holder to pop out our handles across multiple reads
 * while minimizing pollution to core CommitLogReader code implementation.
 *
 * _Mandatory_ usage of this API is as follows:
 *      0-N)    {@link #readPartial(int limit)}
 *      1)      {@link #readToCompletion()}
 *      NOTE: neither of these callse will {@link #close} this reader. try-with-resources is the correct usage.
 *      to correctly close out.
 *
 * As this is intended to be used both in an internal C* state as well as by external users looking to read CommitLogSegments,
 * we allow construction to fail gracefully and indicate usability through {@link #isClosed()}.
 */
@NotThreadSafe
public class ResumableCommitLogReader implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);

    /** We hold a reference to these so we can re-use them for subsequent descriptor parsing on resumed reads */
    final File segmentFile;
    final CommitLogDescriptor descriptor;
    private final CommitLogReader commitLogReader;
    final CommitLogReadHandler readHandler;
    final boolean tolerateTruncation;

    /** Can be re-initialized if re-reading a reader w/compression enabled and we're at our known limit. */
    RandomAccessReader rawReader;

    /** We allow the users to determine whether or not the system should continue on various forms of read failure. As
     * such, we allow resumable readers to be constructed even if they are unusable to the end-user. */
    boolean isClosed = false;

    /** Separate sentinel to indicate whether we have read to completion on our underlying file. Flagged by SegmentReader
     * We use this during {@link #reBufferData()} to determine whether or not to recreate our underlying RAR in the compressed
     * case.
     */
    boolean readToExhaustion = false;

    /** Minimum position before which we completely skip CommitLogSegments */
    final CommitLogPosition minPosition;

    /** Sentinel used to limit reads */
    final int mutationLimit;

    /** We cache, snapshot, and revert position inside our {@link CommitLogSegmentReader.SegmentIterator#computeNext} calls
     * to keep the user-facing API simple */
    @Nullable
    Iterator<CommitLogSegmentReader.SyncSegment> activeIterator;

    @Nullable
    private CommitLogSegmentReader segmentReader;

    /** Raw file offset at which we will stop iterating and processing mutations on a read */
    int offsetLimit = Integer.MIN_VALUE;

    public ResumableCommitLogReader(File commitLogSegment, CommitLogReadHandler readHandler) throws IOException, NullPointerException
    {
        this(commitLogSegment, readHandler, CommitLogPosition.NONE, CommitLogReader.ALL_MUTATIONS, true);
    }

    public ResumableCommitLogReader(File commitLogSegment,
                                    CommitLogReadHandler readHandler,
                                    CommitLogPosition minPosition,
                                    int mutationLimit,
                                    boolean tolerateTruncation) throws IOException, NullPointerException
    {
        this.segmentFile = commitLogSegment;
        this.commitLogReader = new CommitLogReader();
        this.readHandler = readHandler;
        this.mutationLimit = mutationLimit;
        this.minPosition = minPosition;
        this.tolerateTruncation = tolerateTruncation;

        Pair<Optional<CommitLogDescriptor>, Integer> header = CommitLogReader.readCommitLogDescriptor(readHandler,
                                                                                                      commitLogSegment,
                                                                                                      tolerateTruncation);
        // Totally fail out if we fail to parse this CommitLogSegment descriptor
        if (!header.left.isPresent())
            throw new RuntimeException(MessageFormat.format("Failed to parse the CommitLogDescriptor from {0}", commitLogSegment));
        descriptor = header.left.get();

        if (shouldSkipSegmentId(new File(descriptor.fileName()), descriptor, minPosition))
        {
            close();
        }
        else
        {
            try
            {
                this.rawReader = RandomAccessReader.open(commitLogSegment);
                rawReader.seek(header.right);

                // This is where we grab and old open our handles if we succeed
                segmentReader = CommitLogReader.getCommitLogSegmentReader(this);
                if (segmentReader != null)
                    this.activeIterator = segmentReader.iterator();
            }
            finally
            {
                if (segmentReader == null)
                    close();
            }
        }
    }

    /**
     * Performs a partial CommitLogSegment read. Closes down this resumable reader on read error.
     *
     * @param readLimit How far to read into the file before stopping.
     */
    public void readPartial(int readLimit) throws IOException
    {
        if (isClosed)
        {
            logger.warn("Attempted to use invalid ResumableCommitLogReader for file {}. Ignoring.", descriptor.fileName());
            return;
        }

        if (readLimit <= offsetLimit)
        {
            logger.warn("Attempted to resume reading a commit log but used already read offset: {}", readLimit);
            return;
        }
        offsetLimit = readLimit;
        rebufferAndRead();
    }

    /** Reads to end of file from current cached offset. */
    public void readToCompletion() throws IOException
    {
        if (isClosed)
        {
            logger.warn("Attempted to use invalid ResumableCommitLogReader for file {}. Ignoring.", descriptor.fileName());
            return;
        }
        offsetLimit = CommitLogReader.READ_TO_END_OF_FILE;
        rebufferAndRead();
    }

    public void close()
    {
        isClosed = true;
        if (rawReader != null)
            rawReader.close();
        segmentReader = null;
        activeIterator = null;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * When we have compression enabled, RandomAccessReader's have CompressionMetadata to indicate the end of their file
     * length. For our purposes, this means we have some difficulty in re-using previously constructed underlying buffers
     * for decompression and reading, so if the underlying file length has changed because a file is actively being written
     * and we've exhausted the current data we know about, we close out our RAR and construct a new one with the new
     * {@link org.apache.cassandra.io.compress.CompressionMetadata}. While it would arguably be better to extend the
     * hierarchy to have a rebuffering compressed segment, YAGNI for now. The added gc pressure from this + overhead
     * on closing and re-opening RAR's should be restricted to non-node partial/resumed CL reading cases which we expect
     * to have very different properties than critical path log replay on a running node, for example.
     */
    private void reBufferData() throws FileNotFoundException
    {
        if (readToExhaustion)
        {
            long toSeek = rawReader.getPosition();
            this.rawReader.close();
            if (!segmentFile.exists())
                throw new FileNotFoundException(String.format("Attempting to reBufferData but underlying file cannot be found: {}",
                                                              segmentFile.getAbsolutePath()));
            this.rawReader = RandomAccessReader.open(segmentFile);
            this.rawReader.seek(toSeek);
        }
        else
        {
            rawReader.reBuffer();
        }
    }

    /** Performs the read operation and closes down this reader on exception. */
    private void rebufferAndRead() throws RuntimeException, IOException
    {
        reBufferData();

        try
        {
            commitLogReader.internalReadCommitLogSegment(this);
        }
        catch (RuntimeException | IOException e)
        {
            close();
            throw e;
        }
    }

    /** Any segment with id >= minPosition.segmentId is a candidate for read. */
    private boolean shouldSkipSegmentId(File file, CommitLogDescriptor desc, CommitLogPosition minPosition)
    {
        logger.debug("Reading {} (CL version {}, messaging version {}, compression {})",
            file.getPath(),
            desc.version,
            desc.getMessagingVersion(),
            desc.compression);

        if (minPosition.segmentId > desc.id)
        {
            logger.trace("Skipping read of fully-flushed {}", file);
            return true;
        }
        return false;
    }

    /** Flag to indicate how the {@link CommitLogSegmentReader.SegmentIterator} should behave on failure to compute next
     * segments.
     */
    boolean isPartial()
    {
        return offsetLimit != CommitLogReader.READ_TO_END_OF_FILE;
    }

    @Override
    public String toString()
    {
        return new StringBuilder()
            .append("File: ").append(descriptor.fileName()).append(", ")
            .append("minPos: ").append(minPosition).append(", ")
            .append("offsetLimit: ").append(offsetLimit).append(", ")
            .append("readerPos: ").append(rawReader.getPosition()).append(", ")
            .append("activeIter: ").append(activeIterator)
            .toString();
    }
}
