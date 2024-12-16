/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.statements;

import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.SelectSizeStatement;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SelectSizeCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.RandomHelpers;

import static org.junit.Assert.assertTrue;

public class LongSelectSizeTest
{
    private static final String KEYSPACE1 = "LongSizeTest";
    private static final String TABLE1 = "LargeTypeSizeTable";
    private static final int COLUMN_COUNT = 100;
    private static final long TEST_TARGET_SIZE = FileUtils.ONE_GIB;

    static int ORIGINAL_FAILURE_THRESHOLD;
    static int ORIGINAL_WARN_THRESHOLD;
    static int ORIGINAL_THROUGHPUT_LIMIT;

    @BeforeClass
    public static void setUp() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1));

        ORIGINAL_FAILURE_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
        ORIGINAL_WARN_THRESHOLD = DatabaseDescriptor.getTombstoneFailureThreshold();
        ORIGINAL_THROUGHPUT_LIMIT = DatabaseDescriptor.getSelectSizeThroughputMebibytesPerSec();

        // We don't want our live estimate size calculation to butt up against TombstoneOverwhelmingExceptions
        DatabaseDescriptor.setTombstoneFailureThreshold(Integer.MAX_VALUE);
        DatabaseDescriptor.setTombstoneWarnThreshold(Integer.MAX_VALUE);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setTombstoneFailureThreshold(ORIGINAL_FAILURE_THRESHOLD);
        DatabaseDescriptor.setTombstoneWarnThreshold(ORIGINAL_WARN_THRESHOLD);
        DatabaseDescriptor.setSelectSizeThroughputMebibytesPerSec(ORIGINAL_THROUGHPUT_LIMIT);
    }

    @Before
    public void beforeTest()
    {
        RandomHelpers.setSeed();
    }

    @After
    public void afterTest()
    {
        SchemaTestUtil.announceTableDrop(KEYSPACE1, TABLE1);
    }

    @Test
    public void testIntCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(Int32Type.instance);
        RandomHelpers.printSeed("testIntCalculation");
        runTest(cfs, calculateRowCount(Integer.SIZE / 4), simpleRowGenerator(cfs, RandomHelpers::nextInt));
    }

    @Test
    public void testStringCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(AsciiType.instance);
        RandomHelpers.printSeed("testStringCalculation");
        runTest(cfs, calculateRowCount(100), simpleRowGenerator(cfs, () -> RandomHelpers.makeRandomString(100)));
    }

    @Test
    public void testBlobCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(BytesType.instance);
        RandomHelpers.printSeed("testBlobCalculation");

        Consumer<Integer> byteRowBuilder = (a) -> {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata.get(), 0, String.valueOf(0)).clustering(String.valueOf(a));
            for (int i = 0; i < COLUMN_COUNT; i++)
            {
                byte[] buffer = new byte[100];
                RandomHelpers.nextBytes(buffer);
                builder.add("val" + i, ByteBuffer.wrap(buffer));
            }
            builder.build().apply();
        };

        // Funny thing about completely random byte arrays: they're not compressible. So we need to invert our check
        // in the test method to expect compressed size to be _larger_ than uncompressed.
        runTest(cfs, calculateRowCount(100), byteRowBuilder, simpleDeletionGenerator, compressionClose);
    }

    @Test
    public void testBigIntCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(IntegerType.instance);
        RandomHelpers.printSeed("testBigIntCalculation");
        runTest(cfs, calculateRowCount(100), simpleRowGenerator(cfs, () -> RandomHelpers.nextBoundedBigInteger(100)));
    }

    @Test
    public void testBigDecimalCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(DecimalType.instance);
        RandomHelpers.printSeed("testBigDecimalCalculation");
        runTest(cfs, calculateRowCount(100), simpleRowGenerator(cfs, () -> RandomHelpers.nextBoundedBigDecimal(100, 100)));
    }

    @Test
    public void testLongCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(LongType.instance);
        RandomHelpers.printSeed("testLongCalculation");
        runTest(cfs, calculateRowCount(Long.SIZE / 4), simpleRowGenerator(cfs, RandomHelpers::nextLong));
    }

    @Test
    public void testDateCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(SimpleDateType.instance);
        RandomHelpers.printSeed("testDateCalculation");
        runTest(cfs, calculateRowCount(Long.SIZE / 4), simpleRowGenerator(cfs, RandomHelpers::nextInt));
    }

    @Test
    public void testUUIDCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(TimeUUIDType.instance);
        RandomHelpers.printSeed("testUUIDCalculation");
        runTest(cfs, calculateRowCount((Long.SIZE * 2) / 4), simpleRowGenerator(cfs, RandomHelpers::nextUUID));
    }

    @Test
    public void testVarCharCalculation() throws ConfigurationException
    {
        ColumnFamilyStore cfs = buildMetaDataAndAnnounce(UTF8Type.instance);
        RandomHelpers.printSeed("testVarCharCalculation");
        // Should average 50 bytes in range of 1-100
        runTest(cfs, calculateRowCount(50), simpleRowGenerator(cfs, () -> RandomHelpers.makeRandomStringBounded(100)));
    }

    /** Ensure the columns we're using in our complex test stay in sync with {@link #complexColumnNames} */
    @Test
    public void testComplexMixedCalculation() throws ConfigurationException
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE1, TABLE1)
                                                     .addPartitionKeyColumn("key", AsciiType.instance)
                                                     .addClusteringColumn("name", AsciiType.instance)
                                                     .addRegularColumn("asciitype", AsciiType.instance) // 1024
                                                     .addRegularColumn("int32type", Int32Type.instance) // 4
                                                     .addRegularColumn("longtype", LongType.instance) // 8
                                                     .addRegularColumn("datetype", SimpleDateType.instance) // 4
                                                     .addRegularColumn("uuidtype", UUIDType.instance) // 16
                                                     .addRegularColumn("utf8type", UTF8Type.instance) // 1024
                                                     .addRegularColumn("integertype", IntegerType.instance) // ~50 on average
                                                     .addRegularColumn("bigdecimaltype", DecimalType.instance); // ~100 on average
        long roughRowSize = 8 + 8 + 1024 + 4 + 8 + 4 + 16 + 1024 + 50 + 100;

        TableMetadata complexMetadata = builder.build();
        SchemaTestUtil.announceNewTable(complexMetadata);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(complexMetadata.name);

        Consumer<Integer> complexRowBuilder = (a) -> {
            RowUpdateBuilder rowBuilder = new RowUpdateBuilder(cfs.metadata.get(), 0, String.valueOf(0))
                .clustering(String.valueOf(a))
                .add("asciitype", RandomHelpers.makeRandomStringBounded(2048))
                .add("int32type", RandomHelpers.nextInt())
                .add("longtype", RandomHelpers.nextLong())
                .add("datetype", RandomHelpers.nextInt())
                .add("uuidtype", RandomHelpers.nextUUID())
                .add("utf8type", RandomHelpers.makeRandomStringBounded(2048))
                .add("integertype", RandomHelpers.nextBoundedBigInteger(50))
                .add("bigdecimaltype", RandomHelpers.nextBoundedBigDecimal(50, 50));
            rowBuilder.build().apply();
        };

        runTest(cfs, calculateRowCount(roughRowSize), complexRowBuilder, complexDeletionGenerator, compressionSmaller);
    }

    private Consumer<Integer> simpleRowGenerator(ColumnFamilyStore cfs, Supplier<?> typeGen)
    {
        return (a) -> {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata.get(), 0, String.valueOf(0)).clustering(String.valueOf(a));
            for (int i = 0; i < COLUMN_COUNT; i++)
                builder.add("val" + i, String.valueOf(typeGen.get()));
            builder.build().apply();
        };
    }

    /**
     * Since not all the tests will have the same relationship of uncompressed to compressed data, this helper tidies
     * things up a touch.
     */
    private static class CompressionComparison
    {
        public final String operator;
        public BiFunction<Long, Long, Boolean> test;

        /**
         * @param test First arg: compressed size, 2nd arg: other size
         */
        public CompressionComparison(String operator, BiFunction<Long, Long, Boolean> test)
        {
            this.operator = operator;
            this.test = test;
        }
    }

    private static final CompressionComparison compressionSmaller = new CompressionComparison("<", (a, b) -> a < b);
    private static final CompressionComparison compressionClose = new CompressionComparison("~", (a, b) -> Math.abs(((float)a - (float)b) / (float)a) <= .05f);

    private static final Function<String, String> simpleDeletionGenerator = (index) -> "val" + index;

    /** Needs to match the columns created and used in {@link #testComplexMixedCalculation} */
    static String[] complexColumnNames = new String[] {
        "asciitype",
        "int32type",
        "longtype",
        "datetype",
        "uuidtype",
        "utf8type",
        "integertype",
        "bigdecimaltype" };
    private static final Function<String, String> complexDeletionGenerator = (index) -> complexColumnNames[RandomHelpers.nextInt(complexColumnNames.length)];

    private void runTest(ColumnFamilyStore cfs, int rowCount, Consumer<Integer> rowGenerator)
    {
        runTest(cfs, rowCount, rowGenerator, simpleDeletionGenerator, compressionSmaller);
    }

    private void runTest(ColumnFamilyStore cfs, int rowCount, Consumer<Integer> rowGenerator, Function<String, String> columnNameGenerator, CompressionComparison comparisonHelper)
    {
        for (int i = 0; i < rowCount; i++)
            rowGenerator.accept(i);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        ByteBuffer key = ByteBuffer.wrap(String.valueOf(0).getBytes());

        SelectSizeCommand uCMD = new SelectSizeCommand(KEYSPACE1, cfs.name, key, SelectSizeStatement.Type.UNCOMPRESSED);
        long uSize = uCMD.executeLocally();

        SelectSizeCommand cCMD = new SelectSizeCommand(KEYSPACE1, cfs.name, key, SelectSizeStatement.Type.COMPRESSED);
        long cSize = cCMD.executeLocally();

        SelectSizeCommand eCMD = new SelectSizeCommand(KEYSPACE1, cfs.name, key, SelectSizeStatement.Type.LIVE_UNCOMPRESSED);
        long eSize = eCMD.executeLocally();

        logSizesIfIntellij(uSize, cSize, eSize);

        assertTrue("CompressedSize is not " + comparisonHelper.operator + " UncompressedSize. Compressed: " + pp(cSize) + ". Uncompressed: " + pp(uSize), comparisonHelper.test.apply(cSize, uSize));
        assertTrue("CompressedSize is not " + comparisonHelper.operator + " EstimatedLive size. Compressed: " + pp(cSize) + ". Estimated Live: " + pp(eSize), comparisonHelper.test.apply(cSize, eSize));

        long absDiff = Math.abs(uSize - eSize);
        assertTrue("EstimatedLive size is not within 10% of UncompressedSize. Estimate: " + pp(eSize) + ". Uncompressed: " + pp(uSize), absDiff / (float) uSize < SelectSizeCommand.ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN);

        for (int i = 0; i < rowCount; i += 2)
            RowUpdateBuilder.deleteRow(cfs.metadata.get(), 10, String.valueOf(0), String.valueOf(i)).apply();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        uSize = uCMD.executeLocally();
        cSize = cCMD.executeLocally();
        eSize = eCMD.executeLocally();

        // In the case of completely random bytes, we expect the compressed to non-compressed comparison to be quite close to one another.
        assertTrue("compressedSize is not " + comparisonHelper.operator + " uncompressedSize. Compressed: " + pp(cSize) + ". Uncompressed: " + pp(uSize), comparisonHelper.test.apply(cSize, uSize));

        // We can't draw any conclusions or assert on the compressed vs. estimated live in this case as compression ratios may be
        // good enough to still be < 50% of the uncompressed live on disk. See: UUID's.

        long halfTarget = uSize / 2;
        absDiff = Math.abs(halfTarget - eSize);
        assertTrue("EstimatedLive size is not within 10% of 1/2 the size of uncompressed live after deletion. Uncompressed: " + pp(uSize) + ". Estimated live: " + pp(eSize),
                   absDiff / (float) halfTarget < SelectSizeCommand.ALLOWABLE_LIVE_UNCOMPRESSED_ERROR_MARGIN);

        logSizesIfIntellij(uSize, cSize, eSize);

        long preESize = eSize;

        // Now delete some individual cells and ensure that the estimated live size calculation picks that up as well
        for (int i = 0; i < rowCount; i += 5)
        {
            for (int j = 0; j < COLUMN_COUNT; j += 5)
            {
                RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata.get(), 0, String.valueOf(0)).clustering(String.valueOf(i));
                builder.delete(columnNameGenerator.apply(String.valueOf(j)));
                builder.build().apply();
            }
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        eSize = eCMD.executeLocally();
        Util.logIfIntellij("Pre Cell Delete Size:  " + pp(preESize));
        Util.logIfIntellij("Post Cell Delete Size: " + pp(eSize));
        assertTrue("Cell deletion didn't appear in our estimate calculation update", eSize < preESize);

        // If someone created an instance of this test with either tiny sizes or fat rows, the next test doesn't make sense.
        if (rowCount < 2)
            return;

        preESize = eSize;
        // And finally, range delete some clusterings and ensure that estimated is further shrunk
        // If we have so few rows left because test size and row count calc were small, just exit.
        ClusteringComparator comp = cfs.getComparator();

        DeletionTime deletionTime = DeletionTime.build(Clock.Global.currentTimeMillis(), FBUtilities.nowInSeconds());
        new RowUpdateBuilder(cfs.metadata.get(), 1, String.valueOf(0))
                             .addRangeTombstone(new RangeTombstone(Slice.make(comp.make("0"), comp.make(String.valueOf(rowCount / 2))), deletionTime))
                             .build().apply();

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        eSize = eCMD.executeLocally();
        Util.logIfIntellij("preESize:         " + pp(preESize));
        Util.logIfIntellij("Range Delete Size:" + pp(eSize));
        assertTrue("Range deletion didn't appear in our estimate calculation update", eSize < preESize);
    }

    private int calculateRowCount(long typeSize)
    {
        return Math.round(TEST_TARGET_SIZE / (float)(typeSize * COLUMN_COUNT));
    }

    private ColumnFamilyStore buildMetaDataAndAnnounce(AbstractType<?> valType)
    {
        TableMetadata md = SchemaLoader.standardCFMD(KEYSPACE1, TABLE1, COLUMN_COUNT, AsciiType.instance, valType, AsciiType.instance).compression(CompressionParams.lz4()).build();
        SchemaTestUtil.announceNewTable(md);
        return Keyspace.open(KEYSPACE1).getColumnFamilyStore(md.name);
    }

    private void logSizesIfIntellij(long uncompressedSize, long compressedSize, long estimatedLiveSize)
    {
        Util.logIfIntellij("uSize: " + pp(uncompressedSize));
        Util.logIfIntellij("cSize: " + pp(compressedSize));
        Util.logIfIntellij("eSize: " + pp(estimatedLiveSize));
    }

    private String pp(long input)
    {
        return FBUtilities.prettyPrintMemory(input);
    }
}