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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.tcm.log.MetadataLogJournal.Channels;
import org.apache.cassandra.tcm.log.MetadataLogJournal.Contents;
import org.apache.cassandra.tcm.log.MetadataLogJournal.JournalFile;
import org.apache.cassandra.tcm.log.MetadataLogJournal.Record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetadataLogJournalTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path dir;
    private RecordingChannels channels;
    private MetadataLogJournal journal;

    /** Wraps the real file channels, records every call, and fails the next call of one kind on request. */
    static class RecordingChannels implements Channels
    {
        final List<String> events = new ArrayList<>();
        volatile String failNext;

        private void event(String op) throws IOException
        {
            events.add(op);
            if (op.equals(failNext))
            {
                failNext = null;
                throw new IOException("injected " + op + " failure");
            }
        }

        @Override
        public JournalFile create(Path file) throws IOException
        {
            event("create");
            JournalFile real = MetadataLogJournal.FILE_CHANNELS.create(file);
            return new JournalFile()
            {
                @Override
                public void write(ByteBuffer buffer) throws IOException
                {
                    event("write");
                    real.write(buffer);
                }

                @Override
                public void force() throws IOException
                {
                    event("force");
                    real.force();
                }

                @Override
                public void close() throws IOException
                {
                    real.close();
                }
            };
        }

        @Override
        public void syncDirectory(Path directory) throws IOException
        {
            event("syncDir");
            MetadataLogJournal.FILE_CHANNELS.syncDirectory(directory);
        }
    }

    @Before
    public void setup() throws IOException
    {
        dir = folder.newFolder().toPath().resolve("system").resolve("metadata-log-journal");
        channels = new RecordingChannels();
        journal = newJournal();
    }

    private MetadataLogJournal newJournal()
    {
        return new MetadataLogJournal(() -> dir, channels);
    }

    private static Record record(long epoch)
    {
        byte[] payload = new byte[10 + (int) epoch];
        for (int i = 0; i < payload.length; i++)
            payload[i] = (byte) (epoch + i);
        return new Record(epoch, 1000 + epoch, (int) (epoch % 7), ByteBuffer.wrap(payload));
    }

    private void append(long... epochs)
    {
        for (long epoch : epochs)
            journal.append(record(epoch), () -> {});
    }

    private static void assertSame(Record expected, Record actual)
    {
        assertEquals(expected.epoch, actual.epoch);
        assertEquals(expected.entryId, actual.entryId);
        assertEquals(expected.kindId, actual.kindId);
        assertEquals(expected.transformation, actual.transformation);
    }

    private void assertRead(long... epochs) throws IOException
    {
        Contents contents = newJournal().read(false);
        assertEquals(epochs.length, contents.records.size());
        for (int i = 0; i < epochs.length; i++)
            assertSame(record(epochs[i]), contents.records.get(i));
    }

    private List<Path> files() throws IOException
    {
        return newJournal().read(false).files;
    }

    private byte[] bytes(Path file) throws IOException
    {
        return Files.readAllBytes(file);
    }

    private void write(Path file, byte[] bytes) throws IOException
    {
        Files.write(file, bytes);
    }

    private static byte[] prefix(byte[] bytes, int length)
    {
        byte[] out = new byte[length];
        System.arraycopy(bytes, 0, out, 0, length);
        return out;
    }

    @Test
    public void constructorDoesNoDiskAccess()
    {
        AtomicBoolean asked = new AtomicBoolean();
        new MetadataLogJournal(() -> {
            asked.set(true);
            return dir;
        }, channels);
        assertFalse(asked.get());
        assertFalse(Files.exists(dir));
        assertTrue(channels.events.isEmpty());
    }

    @Test
    public void appendAndReadRoundTrip() throws IOException
    {
        append(1, 2, 3);
        assertRead(1, 2, 3);
        assertEquals(1, files().size());
    }

    @Test
    public void readsNothingFromAMissingDirectory() throws IOException
    {
        Contents contents = journal.read(false);
        assertTrue(contents.files.isEmpty());
        assertTrue(contents.records.isEmpty());
    }

    @Test
    public void recordIsForcedBeforeAppendReturns()
    {
        append(1, 2);
        // The new directory is synced (its parent), then the file is created, gets its header, and the directory is
        // synced again, all before the first record. Each record is written and then forced.
        assertEquals(List.of("syncDir", "create", "write", "force", "syncDir", "write", "force", "write", "force"),
                     channels.events);
    }

    @Test
    public void everyAppendEndsWithWriteThenForce()
    {
        append(1);
        int before = channels.events.size();
        append(2, 3, 4);
        List<String> later = channels.events.subList(before, channels.events.size());
        assertEquals(List.of("write", "force", "write", "force", "write", "force"), later);
    }

    @Test
    public void failedBeforeWriteWritesNothingAndDoesNotFailTheJournal() throws IOException
    {
        append(1);
        int before = channels.events.size();
        try
        {
            journal.append(record(2), () -> { throw new IllegalStateException("insert failed"); });
            fail();
        }
        catch (IllegalStateException expected)
        {
            assertEquals("insert failed", expected.getMessage());
        }
        assertEquals(before, channels.events.size());
        append(3);
        assertRead(1, 3);
    }

    @Test
    public void tornLastRecordIsDroppedAtEveryOffset() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        int recordTwoStart = full.length - (4 + 20 + record(2).transformation.remaining() + 4);
        for (int length = recordTwoStart; length < full.length; length++)
        {
            write(file, prefix(full, length));
            assertRead(1);
        }
    }

    @Test
    public void damagedLastRecordIsDropped() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        full[full.length - 1] ^= 0x55;
        write(file, full);
        assertRead(1);
    }

    @Test
    public void tornNewestHeaderIsDroppedAtEveryOffset() throws IOException
    {
        append(1);
        journal.roll();
        append(2);
        List<Path> files = files();
        Path newest = files.get(1);
        byte[] full = bytes(newest);
        for (int length = 0; length < 12; length++)
        {
            write(newest, prefix(full, length));
            assertRead(1);
        }
        byte[] garbage = prefix(full, 12);
        garbage[0] ^= 0x7f;
        write(newest, garbage);
        assertRead(1);
    }

    @Test
    public void emptyNewestFileIsDropped() throws IOException
    {
        append(1);
        journal.roll();
        append(2);
        write(files().get(1), new byte[0]);
        assertRead(1);
    }

    @Test
    public void zeroFilledTailIsTheEndOfTheLog() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        byte[] padded = Arrays.copyOf(full, full.length + 100);
        write(file, padded);
        assertRead(1, 2);

        // A torn append whose first page stayed zero while a later page reached disk
        padded[padded.length - 1] = 1;
        write(file, padded);
        assertRead(1, 2);
    }

    @Test
    public void tornRecordWithAZeroLengthIsDropped() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        int recordTwoStart = full.length - (4 + 20 + record(2).transformation.remaining() + 4);
        ByteBuffer.wrap(full).putInt(recordTwoStart, 0);
        write(file, full);
        assertRead(1);
    }

    @Test
    public void damagedRecordFollowedByAValidOneInTheNewestFileFails() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        ByteBuffer.wrap(full).putInt(12, 0); // the length of record 1
        write(file, full);
        try
        {
            newJournal().read(false);
            fail();
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("followed by a valid record"));
        }
    }

    @Test
    public void lengthLargerThanTheFileIsATornTail() throws IOException
    {
        append(1, 2);
        Path file = files().get(0);
        byte[] full = bytes(file);
        int recordTwoStart = full.length - (4 + 20 + record(2).transformation.remaining() + 4);
        ByteBuffer.wrap(full).putInt(recordTwoStart, Integer.MAX_VALUE);
        write(file, full);
        assertRead(1);
    }

    @Test
    public void damagedRecordNotAtTheEndFails() throws IOException
    {
        append(1, 2, 3);
        Path file = files().get(0);
        byte[] full = bytes(file);
        full[12 + 4 + 8 + 2] ^= 0x55; // inside record 1
        write(file, full);
        try
        {
            newJournal().read(false);
            fail();
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("damaged record"));
        }
    }

    @Test
    public void tornEndOfAFileThatIsNotTheNewestFails() throws IOException
    {
        append(1, 2);
        journal.roll();
        append(3);
        Path older = files().get(0);
        byte[] full = bytes(older);
        write(older, prefix(full, full.length - 3));
        try
        {
            newJournal().read(false);
            fail();
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("not the newest"));
        }
    }

    @Test
    public void unknownFormatVersionFails() throws IOException
    {
        append(1);
        Path file = files().get(0);
        byte[] full = bytes(file);
        ByteBuffer buffer = ByteBuffer.wrap(full);
        buffer.putInt(4, 2);
        CRC32 crc = new CRC32();
        crc.update(full, 0, 8);
        buffer.putInt(8, (int) crc.getValue());
        write(file, full);
        try
        {
            newJournal().read(false);
            fail();
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("format version 2"));
        }
    }

    @Test
    public void skipBadFilesKeepsTheRest() throws IOException
    {
        append(1, 2);
        journal.roll();
        append(3);
        Path older = files().get(0);
        byte[] full = bytes(older);
        full[12 + 4 + 8 + 2] ^= 0x55; // inside record 1: bad, and not at the end
        write(older, full);

        Contents contents = newJournal().read(true);
        assertEquals(List.of(older), contents.skipped);
        assertEquals(2, contents.files.size());
        assertEquals(1, contents.records.size());
        assertSame(record(3), contents.records.get(0));
    }

    @Test
    public void rollClosesTheFileAndTheNextAppendOpensANewOne() throws IOException
    {
        assertTrue(journal.roll().isEmpty());
        append(1, 2);
        List<Path> closed = journal.roll();
        assertEquals(files(), closed);
        assertTrue(journal.roll().isEmpty());
        append(3);
        List<Path> files = files();
        assertEquals(2, files.size());
        assertEquals(closed.get(0), files.get(0));
        assertRead(1, 2, 3);

        journal.delete(closed);
        assertEquals(List.of(files.get(1)), files());
        assertRead(3);
    }

    @Test
    public void sequenceContinuesAfterExistingFiles() throws IOException
    {
        append(1);
        journal.roll();
        MetadataLogJournal restarted = newJournal();
        restarted.append(record(2), () -> {});
        List<Path> files = files();
        assertEquals(2, files.size());
        assertTrue(files.get(0).getFileName().toString().compareTo(files.get(1).getFileName().toString()) < 0);
        assertRead(1, 2);
    }

    @Test
    public void truncateDeletesEveryFile() throws IOException
    {
        append(1);
        journal.roll();
        append(2);
        journal.truncate();
        assertTrue(files().isEmpty());
        append(3);
        assertRead(3);
    }

    @Test
    public void everyIoFailureThrowsFSWriteErrorAndPoisonsTheJournal() throws IOException
    {
        for (String op : new String[]{ "create", "write", "force", "syncDir" })
        {
            setup();
            append(1);
            journal.roll();
            channels.failNext = op;
            try
            {
                append(2); // the new file: create, header write, header force, directory sync
                fail(op);
            }
            catch (FSWriteError expected)
            {
                // expected
            }
            int events = channels.events.size();
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    append(3 + i);
                    fail(op);
                }
                catch (FSWriteError expected)
                {
                    // expected
                }
            }
            assertEquals("no I/O after the first failure, " + op, events, channels.events.size());
            assertTrue(journal.roll().isEmpty());
            newJournal().read(false); // the damaged end is a torn tail: recovery accepts it
        }
    }

    @Test
    public void recordForceFailureIsNeverRetriedOnTheSameFile() throws IOException
    {
        append(1);
        channels.failNext = "force";
        try
        {
            append(2);
            fail();
        }
        catch (FSWriteError expected)
        {
            // expected
        }
        int events = channels.events.size();
        try
        {
            append(3); // the injected failure is spent, so only the poisoned state can stop this append
            fail();
        }
        catch (FSWriteError expected)
        {
            // expected
        }
        assertEquals(events, channels.events.size());
        // record 2 was written but its force failed: it was never acknowledged, and recovery may keep it
        assertEquals(2, newJournal().read(false).records.size());
    }
}
