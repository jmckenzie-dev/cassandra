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

package org.apache.cassandra.tcm.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.utils.SyncUtil;

/**
 * An append-only journal of local metadata log entries. {@link #append} forces each record before it returns, so a
 * record that was acknowledged survives a crash. See {@link SystemKeyspaceStorage} for how it is used.
 * <p>
 * File {@code journal-<sequence>.log}: a header (magic, format version, CRC32 of both), then records. A record is
 * {@code length} (int, the bytes from epoch to the end of the transformation), epoch (long), entry id (long), kind id
 * (int), the versioned transformation bytes, and a CRC32 (int) over everything before it, length included.
 * <p>
 * The first I/O error fails the journal for the life of the process: it throws {@link FSWriteError} and writes
 * nothing more, so only the end of the newest file can be incomplete after a failure or crash. The constructor does
 * no disk access.
 */
public class MetadataLogJournal
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataLogJournal.class);

    public static final String DIRECTORY_NAME = "metadata-log-journal";

    private static final int MAGIC = 0x54434D4A; // "TCMJ"
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 12;
    private static final int PAYLOAD_FIXED_SIZE = 8 + 8 + 4;
    private static final Pattern FILE_NAME = Pattern.compile("journal-(\\d+)\\.log");

    /** A file that the journal writes through. */
    public interface JournalFile extends Closeable
    {
        /** Writes all remaining bytes of the buffer. */
        void write(ByteBuffer buffer) throws IOException;

        void force() throws IOException;
    }

    /** Creates files and syncs directories. A test replaces it to inject failures and to see the call order. */
    public interface Channels
    {
        /** Creates a new file; fails if it exists. */
        JournalFile create(Path file) throws IOException;

        void syncDirectory(Path directory) throws IOException;
    }

    public static final Channels FILE_CHANNELS = new Channels()
    {
        @Override
        public JournalFile create(Path file) throws IOException
        {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return new JournalFile()
            {
                @Override
                public void write(ByteBuffer buffer) throws IOException
                {
                    while (buffer.hasRemaining())
                        channel.write(buffer);
                }

                @Override
                public void force() throws IOException
                {
                    SyncUtil.force(channel, false);
                }

                @Override
                public void close() throws IOException
                {
                    channel.close();
                }
            };
        }

        @Override
        public void syncDirectory(Path directory) throws IOException
        {
            if (SyncUtil.SKIP_SYNC)
                return;
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ))
            {
                channel.force(true);
            }
        }
    };

    public static final class Record
    {
        public final long epoch;
        public final long entryId;
        public final int kindId;
        public final ByteBuffer transformation;

        public Record(long epoch, long entryId, int kindId, ByteBuffer transformation)
        {
            this.epoch = epoch;
            this.entryId = entryId;
            this.kindId = kindId;
            this.transformation = transformation;
        }
    }

    /** What {@link #read} found. {@code skipped} are files that failed validation and were skipped on request. */
    public static final class Contents
    {
        public final List<Path> files = new ArrayList<>();
        public final List<Path> skipped = new ArrayList<>();
        public final List<Record> records = new ArrayList<>();
    }

    private final Supplier<Path> directory;
    private final Channels channels;
    private final Object lock = new Object();

    private JournalFile current;
    private Path currentPath;
    private long nextSequence = -1;
    private FSWriteError failure;

    public MetadataLogJournal(Supplier<Path> directory, Channels channels)
    {
        this.directory = directory;
        this.channels = channels;
    }

    /**
     * Runs {@code beforeWrite}, then writes the record and forces the file, all under the journal lock so that a
     * {@link #roll} cannot fall between them. Throws without writing if {@code beforeWrite} throws. Any I/O error
     * fails the journal and throws {@link FSWriteError}, now and on every later call.
     */
    public void append(Record record, Runnable beforeWrite)
    {
        synchronized (lock)
        {
            checkNotFailed();
            beforeWrite.run();
            ByteBuffer encoded = encode(record);
            try
            {
                if (current == null)
                    openNext();
                current.write(encoded);
                current.force();
            }
            catch (IOException | RuntimeException e)
            {
                throw fail(e);
            }
        }
    }

    /**
     * Closes the current file and returns it; the next append opens a new one. Empty when nothing was appended since
     * the last roll. The caller deletes the files after the table has been flushed.
     */
    public List<Path> roll()
    {
        synchronized (lock)
        {
            if (current == null)
                return List.of();
            Path closed = currentPath;
            closeCurrentQuietly();
            return List.of(closed);
        }
    }

    /**
     * Deletes the files and throws {@link FSWriteError} if one stays. A file that stays after a restart could hold a
     * torn end that is no longer in the newest file, so recovery treats this as a disk error.
     */
    public void delete(List<Path> files)
    {
        for (Path file : files)
        {
            try
            {
                Files.deleteIfExists(file);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        }
    }

    /** Renames a file that failed validation to {@code <name>.bad}, so that it is kept but not read again. */
    public void quarantine(Path file)
    {
        try
        {
            Files.move(file, file.resolveSibling(file.getFileName() + ".bad"));
        }
        catch (IOException e)
        {
            logger.warn("Could not rename the bad metadata log journal file {}", file, e);
        }
    }

    /** Closes the current file and deletes every journal file. For tests. */
    public void truncate()
    {
        synchronized (lock)
        {
            closeCurrentQuietly();
            try
            {
                for (Path file : listFiles())
                    Files.deleteIfExists(file);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, directory.get());
            }
            nextSequence = -1;
        }
    }

    /**
     * Reads every journal file in sequence order. Only the end of the newest file can be incomplete (the append never
     * returned): a header that is short or damaged with no record after it, or a last record that is short or
     * damaged, is dropped with a warning. Any other damage throws {@link IOException}, unless {@code skipBadFiles} is
     * set: then the file is skipped (its records before the damage are kept) and listed in {@code skipped}.
     * Does not modify anything.
     */
    public Contents read(boolean skipBadFiles) throws IOException
    {
        Contents contents = new Contents();
        List<Path> files = listFiles();
        for (int i = 0; i < files.size(); i++)
        {
            Path file = files.get(i);
            contents.files.add(file);
            try
            {
                readFile(file, Files.readAllBytes(file), i == files.size() - 1, contents.records);
            }
            catch (IOException e)
            {
                if (!skipBadFiles)
                    throw e;
                logger.error("Skipping the bad metadata log journal file {}: {}. This loses the acknowledged entries " +
                             "after the damage in this file; the node can start at an older epoch.", file, e.getMessage());
                contents.skipped.add(file);
            }
        }
        return contents;
    }

    private static void readFile(Path file, byte[] bytes, boolean newest, List<Record> records) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (bytes.length < HEADER_SIZE)
        {
            dropOrFail(newest, file, "its header is short");
            return;
        }
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, 8);
        if (buffer.getInt(0) != MAGIC || buffer.getInt(8) != (int) crc.getValue())
        {
            if (bytes.length == HEADER_SIZE)
            {
                dropOrFail(newest, file, "its header is damaged");
                return;
            }
            throw new IOException("Metadata log journal file " + file + " has a damaged header");
        }
        if (buffer.getInt(4) != VERSION)
            throw new IOException("Metadata log journal file " + file + " has format version " + buffer.getInt(4) +
                                  "; this node reads version " + VERSION);

        int position = HEADER_SIZE;
        while (position < bytes.length)
        {
            int recordEnd = validRecordEnd(bytes, position, crc);
            if (recordEnd < 0)
            {
                if (isZeros(bytes, position))
                    return;
                tornTailOrFail(newest, file, bytes, position, crc);
                return;
            }
            int length = buffer.getInt(position);
            int transformationLength = length - PAYLOAD_FIXED_SIZE;
            records.add(new Record(buffer.getLong(position + 4),
                                   buffer.getLong(position + 12),
                                   buffer.getInt(position + 20),
                                   ByteBuffer.wrap(bytes, position + 24, transformationLength).slice()));
            position = recordEnd;
        }
    }

    /** The end of the record at {@code position} if its length fits and its CRC matches, else -1. */
    private static int validRecordEnd(byte[] bytes, int position, CRC32 crc)
    {
        if (bytes.length - position < 4)
            return -1;
        int length = ByteBuffer.wrap(bytes).getInt(position);
        if (length < PAYLOAD_FIXED_SIZE)
            return -1;
        long end = position + 8L + length;
        if (end > bytes.length)
            return -1;
        int recordEnd = (int) end;
        crc.reset();
        crc.update(bytes, position, recordEnd - 4 - position);
        return ByteBuffer.wrap(bytes).getInt(recordEnd - 4) == (int) crc.getValue() ? recordEnd : -1;
    }

    private static boolean isZeros(byte[] bytes, int from)
    {
        for (int i = from; i < bytes.length; i++)
            if (bytes[i] != 0)
                return false;
        return true;
    }

    /**
     * Each record is forced before the next is written, so the unforced last append can leave any bytes at the end of
     * the newest file (its pages can reach disk in any order), but never a valid record after them. A valid record
     * after the damage means forced data is damaged.
     */
    private static void tornTailOrFail(boolean newest, Path file, byte[] bytes, int position, CRC32 crc) throws IOException
    {
        if (!newest)
            throw new IOException("Metadata log journal file " + file + " is not the newest file and has a damaged or short record at byte " + position);
        for (int next = position + 1; next < bytes.length; next++)
            if (validRecordEnd(bytes, next, crc) > 0)
                throw new IOException("Metadata log journal file " + file + " has a damaged record at byte " + position +
                                      " followed by a valid record at byte " + next);
        dropOrFail(true, file, (bytes.length - position) + " bytes at byte " + position + " are not a complete record");
    }

    private static void dropOrFail(boolean newest, Path file, String reason) throws IOException
    {
        if (!newest)
            throw new IOException("Metadata log journal file " + file + " is not the newest file and " + reason);
        logger.warn("Dropping the end of the newest metadata log journal file {}: {}. The append that wrote it never returned.", file, reason);
    }

    private static ByteBuffer encode(Record record)
    {
        ByteBuffer transformation = record.transformation.duplicate();
        int length = PAYLOAD_FIXED_SIZE + transformation.remaining();
        ByteBuffer buffer = ByteBuffer.allocate(4 + length + 4);
        buffer.putInt(length).putLong(record.epoch).putLong(record.entryId).putInt(record.kindId).put(transformation);
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());
        buffer.flip();
        return buffer;
    }

    private static ByteBuffer header()
    {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(MAGIC).putInt(VERSION);
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, 8);
        buffer.putInt((int) crc.getValue());
        buffer.flip();
        return buffer;
    }

    // Caller holds the lock
    private void openNext() throws IOException
    {
        Path dir = directory.get();
        if (nextSequence < 0)
        {
            if (!Files.isDirectory(dir))
            {
                Files.createDirectories(dir);
                channels.syncDirectory(dir.getParent());
            }
            nextSequence = 1;
            for (Path file : listFiles())
                nextSequence = Math.max(nextSequence, sequence(file) + 1);
        }
        currentPath = dir.resolve(String.format("journal-%010d.log", nextSequence));
        current = channels.create(currentPath);
        nextSequence++;
        current.write(header());
        current.force();
        channels.syncDirectory(dir);
    }

    private List<Path> listFiles() throws IOException
    {
        Path dir = directory.get();
        if (!Files.isDirectory(dir))
            return List.of();
        try (Stream<Path> stream = Files.list(dir))
        {
            return stream.filter(p -> FILE_NAME.matcher(p.getFileName().toString()).matches())
                         .sorted((a, b) -> Long.compare(sequence(a), sequence(b)))
                         .toList();
        }
    }

    private static long sequence(Path file)
    {
        Matcher matcher = FILE_NAME.matcher(file.getFileName().toString());
        if (!matcher.matches())
            throw new IllegalArgumentException(file.toString());
        return Long.parseLong(matcher.group(1));
    }

    // Caller holds the lock
    private void checkNotFailed()
    {
        if (failure != null)
            throw new FSWriteError(new IOException("The metadata log journal failed earlier and accepts no more entries", failure), failure.path);
    }

    // Caller holds the lock. The file may hold a partial record, so never write to it again, and never force it again.
    private FSWriteError fail(Throwable cause)
    {
        String path = currentPath != null ? currentPath.toString() : String.valueOf(directory.get());
        failure = new FSWriteError(cause, path);
        closeCurrentQuietly();
        logger.error("The metadata log journal failed at {}; it accepts no more entries until the node restarts", path, cause);
        return failure;
    }

    // Caller holds the lock
    private void closeCurrentQuietly()
    {
        if (current == null)
            return;
        try
        {
            current.close();
        }
        catch (IOException | RuntimeException e)
        {
            logger.warn("Could not close the metadata log journal file {}", currentPath, e);
        }
        current = null;
    }
}
