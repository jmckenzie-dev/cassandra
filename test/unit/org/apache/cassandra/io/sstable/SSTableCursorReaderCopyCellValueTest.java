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
package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.DONE;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.PARTITION_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.STATIC_ROW_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.TOMBSTONE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link SSTableCursorReader#copyCellValue} passes its {@code writer} to
 * {@code copyCellContents}, which fast-paths only on {@code writer instanceof DataOutputBuffer}
 * (SSTableCursorReader.java) and calls {@link DataOutputBuffer#readFully}. That method requires
 * a heap-backed buffer (it dereferences {@code array()}/{@code arrayOffset()} unguarded) but the
 * call site never checks for one: a direct-backed {@code DataOutputBuffer} takes the fast path
 * anyway and blows up, instead of falling back to the transfer-buffer loop that already exists
 * immediately below in the same method.
 *
 * No current production writer is direct-backed (CursorCompactor's temp cell buffers use the
 * heap-backed {@code new DataOutputBuffer()}), so this is dormant today. It documents the
 * unsafe path for any future direct-backed consumer.
 */
public class SSTableCursorReaderCopyCellValueTest extends CQLTester
{
    /** A DataOutputBuffer whose backing buffer is always direct, regardless of allocate_type. */
    private static class DirectDataOutputBuffer extends DataOutputBuffer
    {
        DirectDataOutputBuffer(int size)
        {
            super(size);
        }

        @Override
        protected ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    }

    @Test
    public void copyCellValueSucceedsOnDirectBackedWriter() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        String expectedValue = "the value bytes that must survive the copy";
        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 1L, 1L, expectedValue);
        flush();
        assertTrue("expected exactly one sstable", cfs.getLiveSSTables().size() == 1);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (SSTableCursorReader cursor = new SSTableCursorReader(sstable);
             DataOutputBuffer directWriter = new DirectDataOutputBuffer(16))
        {
            byte[] transfer = new byte[4096];
            PartitionDescriptor pHeader = new PartitionDescriptor(sstable.getPartitioner().createReusableKey(0));
            UnfilteredDescriptor rHeader = new UnfilteredDescriptor(sstable.header.clusteringTypes().toArray(AbstractType[]::new));

            int state = cursor.readPartitionHeader(pHeader);
            byte[] copied = null;
            while (state != DONE)
            {
                while (state != PARTITION_END)
                {
                    switch (state)
                    {
                        case STATIC_ROW_START: state = cursor.readStaticRowHeader(rHeader); break;
                        case ROW_START: state = cursor.readRowHeader(rHeader); break;
                        case TOMBSTONE_START: state = cursor.readTombstoneMarker(rHeader); break;
                        default: throw new IllegalStateException("state " + state);
                    }
                    while (state != UNFILTERED_END && state != PARTITION_END)
                    {
                        if (state == CELL_END) { state = cursor.continueReading(); continue; }
                        if (state != CELL_HEADER_START) break;
                        state = cursor.readCellHeader();
                        if (state == CELL_VALUE_START)
                        {
                            directWriter.clear();
                            // This is the call under test: a direct-backed writer must not throw.
                            state = cursor.copyCellValue(directWriter, transfer);
                            copied = directWriter.toByteArray();
                        }
                    }
                    if (state == UNFILTERED_END)
                        state = cursor.continueReading();
                }
                state = cursor.continueReading();
                if (state != DONE)
                    state = cursor.readPartitionHeader(pHeader);
            }

            // The cursor writes a vint length prefix ahead of a variable-length value (see
            // copyCellContents), the same format AbstractType.writeValue produces for the
            // iterator path.
            try (DataOutputBuffer expectedDob = new DataOutputBuffer())
            {
                UTF8Type.instance.writeValue(ByteBufferUtil.bytes(expectedValue), ByteBufferAccessor.instance, expectedDob);
                assertArrayEquals(expectedDob.toByteArray(), copied);
            }
        }
    }
}
