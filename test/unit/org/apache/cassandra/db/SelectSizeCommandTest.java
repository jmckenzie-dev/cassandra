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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectSizeStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.statements.SelectSizeStatement.Type;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

public class SelectSizeCommandTest
{
    private final static String KS = "ks";
    private final static String TBL = "tbl";
    private static TableMetadata table;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        String compression = "WITH compression = {'class':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':13}";
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        table = CreateTableStatement.parse("CREATE TABLE " + TBL + " (k INT, c INT, t TEXT, i int, j TEXT static, PRIMARY KEY((k), c)) " + compression, KS).build();
        SchemaLoader.createKeyspace(KS, KeyspaceParams.simple(1), table);
    }

    @After
    public void after()
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE TABLE %s.%s", KS, TBL));
    }

    @Test
    public void testCompressedSerialization() throws Exception
    {
        testSerialization(SelectSizeStatement.Type.COMPRESSED);
    }

    @Test
    public void testLiveUncompressedSerialization() throws Exception
    {
        testSerialization(Type.LIVE_UNCOMPRESSED);

    }

    private void testSerialization(Type selectSizeType) throws Exception
    {
        populateData(1, 1);
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush(UNIT_TESTS);

        SelectSizeCommand expected = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(1), selectSizeType);
        long size = SelectSizeCommand.serializer.serializedSize(expected, MessagingService.current_version);

        byte[] bytes = new byte[(int) size];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        DataOutputBuffer out = new DataOutputBufferFixed(bb);
        SelectSizeCommand.serializer.serialize(expected, out, MessagingService.current_version);
        Assert.assertEquals(size, bb.position());

        bb.rewind();
        DataInputBuffer inputBuffer = new DataInputBuffer(bb, true);
        SelectSizeCommand actual = SelectSizeCommand.serializer.deserialize(inputBuffer, MessagingService.current_version);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testCompressedLocalExecution()
    {
        testLocalExecution(SelectSizeStatement.Type.COMPRESSED);
    }

    @Test
    public void testLiveUncompressedLocalExecution()
    {
        testLocalExecution(SelectSizeStatement.Type.LIVE_UNCOMPRESSED);
    }

    private void testLocalExecution(SelectSizeStatement.Type selectSizeType)
    {
        populateData(1, 1);
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush(UNIT_TESTS);

        // no key matching 100, should return 0, shouldn't assert / blow up attempting to use the RowIndexEntry containing calc API's
        SelectSizeCommand cmdE = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(100), selectSizeType);
        long resultE = cmdE.executeLocally();
        Assert.assertEquals(0, resultE);

        // row with key 1, should return non-zero
        SelectSizeCommand cmdF = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(1), selectSizeType);
        long result = cmdF.executeLocally();
        Assert.assertTrue(result > 0);
    }

    @Test
    public void confirmLiveCalculationIncludesStaticRows()
    {
        int key = 100;
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, c, t) VALUES (%d, %d, '%s')",
                                                     KS, TBL,
                                                     key,
                                                     101,
                                                     "Some non-static text."));
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush(UNIT_TESTS);

        SelectSizeCommand cmdN = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(key), SelectSizeStatement.Type.LIVE_UNCOMPRESSED);
        long noStaticSize = cmdN.executeLocally();
        Assert.assertTrue(noStaticSize > 0);

        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, c, j) VALUES (%d, %d, '%s')",
                                                     KS, TBL,
                                                     key,
                                                     101,
                                                     "Inserting some static text."));
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush(UNIT_TESTS);

        SelectSizeCommand cmdS = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(key), SelectSizeStatement.Type.LIVE_UNCOMPRESSED);
        long withStaticSize = cmdS.executeLocally();
        Assert.assertTrue(withStaticSize > noStaticSize);
    }

    @Test
    public void testCompressedIsSmallerThanUncompressed()
    {
        // Populate data for multiple PK's to prompt RowIndexEntry creation
        for (int i = 0; i < 10; i++)
            populateData(i, 1024);
        Schema.instance.getColumnFamilyStoreInstance(table.id).forceBlockingFlush(UNIT_TESTS);

        // no key matching 100001, should return 0, shouldn't assert / blow up attempting to use the RowIndexEntry containing calc API's
        SelectSizeCommand cmdE = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(100000), SelectSizeStatement.Type.COMPRESSED);
        long resultE = cmdE.executeLocally();
        Assert.assertEquals(0, resultE);

        // row with key 1 inserted in population; should have a nice chunk of data and subsequent indexed PK's to check size against
        SelectSizeCommand compCmd = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(1), SelectSizeStatement.Type.COMPRESSED);
        long compressedSize = compCmd.executeLocally();

        SelectSizeCommand uncCmd = new SelectSizeCommand(KS, TBL, ByteBufferUtil.bytes(1), SelectSizeStatement.Type.UNCOMPRESSED);
        long uncompressedSize = uncCmd.executeLocally();
        Assert.assertTrue(compressedSize < uncompressedSize);
    }

    /**
     * Note: you need to {@link ColumnFamilyStore#forceBlockingFlush} after a call to this to get {@link SSTableReader}'s
     * on disk to read through and size. If we flushed in here after every write we end up with a pathological case
     * where we're hitting only 1 {@link CompressionMetadata.Chunk} for a large number of sstables and hitting our edge
     * case to at least indicate a PK compressed down to 1 chunk.
     */
    private void populateData(int key, int count)
    {
        for (int i = 0; i < count; i++)
        {
            // Always insert on the same PK so we can bulk up the data we're checking on the different sizers
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, c, t, i) VALUES (%d, %d, '%s', %d)",
                                                         KS, TBL,
                                                         key,
                                                         i,
                                                         "Inserting some compressible text",
                                                         i));
        }
    }
}
