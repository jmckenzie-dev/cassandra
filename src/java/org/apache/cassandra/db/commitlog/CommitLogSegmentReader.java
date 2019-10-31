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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.zip.CRC32;
import javax.annotation.concurrent.NotThreadSafe;
import javax.crypto.Cipher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadErrorReason;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadException;
import org.apache.cassandra.db.commitlog.EncryptedFileSegmentInputStream.ChunkProvider;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileSegmentInputStream;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.SYNC_MARKER_SIZE;
import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Read each sync section of a commit log, iteratively. Can be run in either one-shot or resumable mode. In resumable,
 * we snapshot the start position of any successful SyncSegment deserialization with the expectation that some reads will
 * land in partially written segments and need to be rolled back to the start of that segment and repeated on further
 * mutation serialization (specifically in encrypted or compressed contexts).
 */
@NotThreadSafe
public class CommitLogSegmentReader implements Iterable<CommitLogSegmentReader.SyncSegment>
{
    private final ResumableCommitLogReader parent;
    private final Segmenter segmenter;

    /** A special SyncSegment we use to indicate / keep our iterators open on a read we intend to resume */
    static final SyncSegment RESUMABLE_SENTINEL = new SyncSegment(null, -1, -1, -1, false);

    /** Ending position of the current sync section. */
    protected int end;

    /**
     * Rather than relying on a formal Builder, this constructs the appropriate type of segment reader (memmap, encrypted,
     * compressed) based on the type stored in the descriptor.
     *
     * Note: If ever using this object directly in a test, ensure you set the {@link ResumableCommitLogReader#offsetLimit}
     * before attempting to use this reader or iteration will never advance.
     */
    CommitLogSegmentReader(ResumableCommitLogReader parent)
    {
        this.parent = parent;

        end = (int) parent.rawReader.getFilePointer();
        if (parent.descriptor.getEncryptionContext().isEnabled())
            segmenter = new EncryptedSegmenter(parent.descriptor, parent);
        else if (parent.descriptor.compression != null)
            segmenter = new CompressedSegmenter(parent.descriptor, parent);
        else
            segmenter = new NoOpSegmenter(parent.rawReader);
    }

    public Iterator<SyncSegment> iterator()
    {
        return new SegmentIterator();
    }

    /** Will return endOfData() or our resumable sentinel depending on what mode the iterator is being used in */
    protected class SegmentIterator extends AbstractIterator<CommitLogSegmentReader.SyncSegment>
    {
        protected SyncSegment computeNext()
        {
            // A couple sanity checks that we're in a good state
            if (parent.offsetLimit == Integer.MIN_VALUE)
                throw new RuntimeException("Attempted to use a CommitLogSegmentReader with an uninitialized ResumableCommitLogReader parent.");

            // Since this could be mis-used by client app parsing code, keep it RTE instead of assertion.
            if (parent.isClosed)
                throw new RuntimeException("Attempted to use a closed ResumableCommitLogReader.");

            while (true)
            {
                try
                {
                    final int currentStart = end;

                    // Segmenters need to know our original state to appropriately roll back on snapshot restore
                    segmenter.stageSnapshot();
                    end = readSyncMarker(parent.descriptor, currentStart, parent.rawReader);

                    if (parent.isPartial())
                    {
                        // Revert our SegmentIterator's state to beginning of last completed SyncSegment read on a partial read.
                        if (end == -1 || end > parent.offsetLimit)
                        {
                            segmenter.revertToSnapshot();
                            end = (int)parent.rawReader.getFilePointer();
                            return RESUMABLE_SENTINEL;
                        }
                        // Flag our RR's data as exhausted if we've hit the end of our reader but think this is partial.
                        else if (end >= parent.rawReader.length())
                        {
                            parent.readToExhaustion = true;
                        }
                    }
                    // Iterate on a non-resumable read.
                    else
                    {
                        if (end == -1)
                        {
                            // We only transition to endOfData if we're doing a non-resumable (i.e. read to end) read,
                            // since it leaves this iterator in a non-reusable state.
                            return endOfData();
                        }
                        else if (end > parent.rawReader.length())
                        {
                            // the CRC was good (meaning it was good when it was written and still looks legit), but the file is truncated now.
                            // try to grab and use as much of the file as possible, which might be nothing if the end of the file truly is corrupt
                            end = (int) parent.rawReader.length();
                        }
                    }

                    // Retain the starting point of this SyncSegment in case we need to roll back a future read to this point.
                    segmenter.takeSnapshot();

                    // Passed the gauntlet. The next segment is cleanly ready for read.
                    return segmenter.nextSegment(currentStart + SYNC_MARKER_SIZE, end);
                }
                catch(CommitLogSegmentReader.SegmentReadException e)
                {
                    try
                    {
                        parent.readHandler.handleUnrecoverableError(new CommitLogReadException(
                                                                    e.getMessage(),
                                                                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                                                    !e.invalidCrc && parent.tolerateTruncation));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
                catch (IOException e)
                {
                    try
                    {
                        boolean tolerateErrorsInSection = parent.tolerateTruncation & segmenter.tolerateSegmentErrors(end, parent.rawReader.length());
                        // if no exception is thrown, the while loop will continue
                        parent.readHandler.handleUnrecoverableError(new CommitLogReadException(
                                                                    e.getMessage(),
                                                                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                                                                    tolerateErrorsInSection));
                    }
                    catch (IOException ioe)
                    {
                        throw new RuntimeException(ioe);
                    }
                }
            }
        }
    }

    /**
     * @return length of this sync segment, -1 if at or beyond the end of file.
     */
    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - SYNC_MARKER_SIZE)
            return -1;
        reader.seek(offset);
        CRC32 crc = new CRC32();
        updateChecksumInt(crc, (int) (descriptor.id & 0xFFFFFFFFL));
        updateChecksumInt(crc, (int) (descriptor.id >>> 32));
        updateChecksumInt(crc, (int) reader.getPosition());
        final int end = reader.readInt();
        long filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                String msg = String.format("Encountered bad header at position %d of commit log %s, with invalid CRC. " +
                             "The end of segment marker should be zero.", offset, reader.getPath());
                throw new SegmentReadException(msg, true);
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            String msg = String.format("Encountered bad header at position %d of commit log %s, with bad position but valid CRC", offset, reader.getPath());
            throw new SegmentReadException(msg, false);
        }
        return end;
    }

    public static class SegmentReadException extends IOException
    {
        public final boolean invalidCrc;

        public SegmentReadException(String msg, boolean invalidCrc)
        {
            super(msg);
            this.invalidCrc = invalidCrc;
        }
    }

    /** The logical unit of data we sync across and read across in CommitLogs. */
    public static class SyncSegment
    {
        /** the 'buffer' to replay commit log data from */
        public final FileDataInput input;

        /** offset in file where this section begins. */
        final int fileStartPosition;

        /** offset in file where this section ends. */
        final int fileEndPosition;

        /** the logical ending position of the buffer */
        final int endPosition;

        final boolean toleratesErrorsInSection;

        SyncSegment(FileDataInput input, int fileStartPosition, int fileEndPosition, int endPosition, boolean toleratesErrorsInSection)
        {
            this.input = input;
            this.fileStartPosition = fileStartPosition;
            this.fileEndPosition = fileEndPosition;
            this.endPosition = endPosition;
            this.toleratesErrorsInSection = toleratesErrorsInSection;
        }
    }

    /**
     * Derives the next section of the commit log to be replayed. Section boundaries are derived from the commit log sync markers.
     * Allows snapshot and resume from snapshot functionality to revert to a "last known good segment" in the event of
     * a partial read on an a file being actively written.
     */
    interface Segmenter
    {
        /**
         * Get the next section of the commit log to replay.
         *
         * @param startPosition the position in the file to begin reading at
         * @param nextSectionStartPosition the file position of the beginning of the next section
         * @return the buffer and it's logical end position
         * @throws IOException
         */
        SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException;

        /**
         * Determine if we tolerate errors in the current segment.
         */
        default boolean tolerateSegmentErrors(int segmentEndPosition, long fileLength)
        {
            return segmentEndPosition >= fileLength || segmentEndPosition < 0;
        }

        /** Holds snapshot data in temporary variables to be finalized when we determine a SyncSegment is fully written */
        void stageSnapshot();

        /** Finalizes snapshot staged in stageSnapshot */
        void takeSnapshot();

        /** Reverts the segmenter to the previously held position. Allows for resumable reads to rollback when they occur
         * in the middle of a SyncSegment. This can be called repeatedly if we have multiple attempts to partially read
         * on an incomplete SyncSegment. */
        void revertToSnapshot();

        /** Visible for debugging only */
        long getSnapshot();
    }

    static class NoOpSegmenter implements Segmenter
    {
        private final RandomAccessReader reader;
        private long snapshotPosition = Long.MIN_VALUE;
        private long stagedSnapshot = Long.MIN_VALUE;

        NoOpSegmenter(RandomAccessReader reader)
        {
            this.reader = reader;
        }

        public SyncSegment nextSegment(int startPosition, int nextSectionStartPosition)
        {
            reader.seek(startPosition);
            return new SyncSegment(reader, startPosition, nextSectionStartPosition, nextSectionStartPosition, true);
        }

        public boolean tolerateSegmentErrors(int end, long length)
        {
            return true;
        }

        public void stageSnapshot()
        {
            stagedSnapshot = reader.getFilePointer();
            // Deal with edge case of initial read attempt being before SyncSegment completion
            if (snapshotPosition == Long.MIN_VALUE)
                takeSnapshot();
        }

        public void takeSnapshot()
        {
            snapshotPosition = stagedSnapshot;
        }

        public void revertToSnapshot()
        {
            reader.seek(snapshotPosition);
        }

        public long getSnapshot()
        {
            return snapshotPosition;
        }
    }

    static class CompressedSegmenter implements Segmenter
    {
        private final ICompressor compressor;
        /** We store a reference to a ResumableReader in the event it needs to re-init and swap out the underlying reader */
        private final ResumableCommitLogReader parent;
        private byte[] compressedBuffer;
        private byte[] uncompressedBuffer;
        private long nextLogicalStart;

        private long stagedLogicalStart = Long.MIN_VALUE;
        private long stagedReaderLocation = Long.MIN_VALUE;
        private long snapshotLogicalStart = Long.MIN_VALUE;
        private long snapshotReaderLocation = Long.MIN_VALUE;

        CompressedSegmenter(CommitLogDescriptor desc, ResumableCommitLogReader parent)
        {
            this(CompressionParams.createCompressor(desc.compression), parent);
        }

        CompressedSegmenter(ICompressor compressor, ResumableCommitLogReader parent)
        {
            this.compressor = compressor;
            this.parent = parent;
            compressedBuffer = new byte[0];
            uncompressedBuffer = new byte[0];
            nextLogicalStart = parent.rawReader.getFilePointer();
        }

        @SuppressWarnings("resource")
        public SyncSegment nextSegment(final int startPosition, final int nextSectionStartPosition) throws IOException
        {
            parent.rawReader.seek(startPosition);
            int uncompressedLength = parent.rawReader.readInt();

            int compressedLength = nextSectionStartPosition - (int)parent.rawReader.getPosition();
            if (compressedLength > compressedBuffer.length)
                compressedBuffer = new byte[(int) (1.2 * compressedLength)];
            parent.rawReader.readFully(compressedBuffer, 0, compressedLength);

            if (uncompressedLength > uncompressedBuffer.length)
               uncompressedBuffer = new byte[(int) (1.2 * uncompressedLength)];
            int count = compressor.uncompress(compressedBuffer, 0, compressedLength, uncompressedBuffer, 0);
            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new FileSegmentInputStream(ByteBuffer.wrap(uncompressedBuffer, 0, count), parent.rawReader.getPath(), nextLogicalStart);
            nextLogicalStart += uncompressedLength;
            return new SyncSegment(input, startPosition, nextSectionStartPosition, (int)nextLogicalStart, tolerateSegmentErrors(nextSectionStartPosition, parent.rawReader.length()));
        }

        public void stageSnapshot()
        {
            stagedLogicalStart = nextLogicalStart;
            stagedReaderLocation = parent.rawReader.getFilePointer();

            // In our default 0 case on a segment w/out anything yet to read, we want to stage the first valid location
            // we've seen, else a resume will kick us to a bad value
            if (snapshotLogicalStart == Long.MIN_VALUE)
                takeSnapshot();
        }

        /** Since {@link #nextLogicalStart} is mutated during decompression but relied upon for decompression, we need
         * to both snapshot and revert that along with the reader's position. */
        public void takeSnapshot()
        {
            snapshotLogicalStart = stagedLogicalStart;
            snapshotReaderLocation = stagedReaderLocation;
        }

        public void revertToSnapshot()
        {
            nextLogicalStart = snapshotLogicalStart;
            parent.rawReader.seek(snapshotReaderLocation);
        }

        public long getSnapshot()
        {
            return snapshotReaderLocation;
        }
    }

    static class EncryptedSegmenter implements Segmenter
    {
        /** We store a reference to a ResumableReader in the event it needs to re-init and swap out the underlying reader */
        private final ResumableCommitLogReader parent;
        private final ICompressor compressor;
        private final Cipher cipher;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer decryptedBuffer;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer uncompressedBuffer;

        private final ChunkProvider chunkProvider;

        private long currentSegmentEndPosition;
        private long nextLogicalStart;

        private long stagedSnapshotPosition;
        private long snapshotPosition;

        EncryptedSegmenter(CommitLogDescriptor descriptor, ResumableCommitLogReader parent)
        {
            this(parent, descriptor.getEncryptionContext());
        }

        @VisibleForTesting
        EncryptedSegmenter(final ResumableCommitLogReader parent, EncryptionContext encryptionContext)
        {
            this.parent = parent;
            decryptedBuffer = ByteBuffer.allocate(0);
            compressor = encryptionContext.getCompressor();
            nextLogicalStart = parent.rawReader.getFilePointer();

            try
            {
                cipher = encryptionContext.getDecryptor();
            }
            catch (IOException ioe)
            {
                throw new FSReadError(ioe, parent.rawReader.getPath());
            }

            chunkProvider = () -> {
                if (parent.rawReader.getFilePointer() >= currentSegmentEndPosition)
                    return ByteBufferUtil.EMPTY_BYTE_BUFFER;
                try
                {
                    decryptedBuffer = EncryptionUtils.decrypt(parent.rawReader, decryptedBuffer, true, cipher);
                    uncompressedBuffer = EncryptionUtils.uncompress(decryptedBuffer, uncompressedBuffer, true, compressor);
                    return uncompressedBuffer;
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, parent.rawReader.getPath());
                }
            };
        }

        @SuppressWarnings("resource")
        public SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException
        {
            int totalPlainTextLength = parent.rawReader.readInt();
            currentSegmentEndPosition = nextSectionStartPosition - 1;

            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new EncryptedFileSegmentInputStream(parent.rawReader.getPath(), nextLogicalStart, 0, totalPlainTextLength, chunkProvider);
            nextLogicalStart += totalPlainTextLength;
            return new SyncSegment(input, startPosition, nextSectionStartPosition, (int)nextLogicalStart, tolerateSegmentErrors(nextSectionStartPosition, parent.rawReader.length()));
        }

        public void stageSnapshot()
        {
            stagedSnapshotPosition = parent.rawReader.getFilePointer();
            if (snapshotPosition == Long.MIN_VALUE)
                takeSnapshot();
        }

        public void takeSnapshot()
        {
            snapshotPosition = stagedSnapshotPosition;
        }

        public void revertToSnapshot()
        {
            parent.rawReader.seek(snapshotPosition);
        }

        public long getSnapshot()
        {
            return snapshotPosition;
        }
    }
}
