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
package org.apache.cassandra.index.sasi.disk;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.EntryType;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.CombinedValue;
import org.apache.cassandra.index.sasi.utils.KeyConverter;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.Pair;

public class TokenTreeTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    final static SortedMap<Long, KeyOffsets> simpleTokensMap = new TreeMap<Long, KeyOffsets>()
    {{
        for (long i = 0; i < 100; i++)
        {
            KeyOffsets offsets = new KeyOffsets();

            for (int j = 0; j < 3; j++)
                offsets.put(i, KeyOffsets.asArray( i + j));

            put(i, offsets);
        }
    }};

    final static SortedMap<Long, KeyOffsets> bigTokensMap = new TreeMap<Long, KeyOffsets>()
    {{
        KeyOffsets offsets = new KeyOffsets() {{
            for (int j = 0; j < 2; j++)
                put(1L, KeyOffsets.asArray(j));
        }};

        for (long i = 0; i < 100000; i++)
            put(i, offsets);
    }};

    final static SortedMap<Long, KeyOffsets> tokensMapWithManyOffsets = new TreeMap<Long, KeyOffsets>()
    {{
        KeyOffsets offsets = new KeyOffsets() {{
            for (int j = 0; j < 500; j++)
                put(1L, KeyOffsets.asArray(j));
        }};

        for (long i = 0; i < 100; i++)
            put(i, offsets);
    }};

    final static SortedMap<Long, KeyOffsets> collidingTokensMap = new TreeMap<Long, KeyOffsets>()
    {{
        KeyOffsets offsets = new KeyOffsets() {{
            for (int i = 0; i < 2; i++)
                for (int j = 0; j < 5; j++)
                    put(i, KeyOffsets.asArray(i + j));
        }};
        for (long i = 0; i < 1000; i++)
            put(i, offsets);
    }};

    final static SequentialWriterOption DEFAULT_OPT = SequentialWriterOption.newBuilder().bufferSize(4096).build();

    List<SortedMap<Long, KeyOffsets>> allTokens = Arrays.asList(simpleTokensMap, bigTokensMap, collidingTokensMap, tokensMapWithManyOffsets);

    @Test
    public void testSerializedSizeDynamic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
        {
            testSerializedSize(new DynamicTokenTreeBuilder(tokens));
        }
    }

    @Test
    public void testSerializedSizeStatic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            testSerializedSize(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)));
    }

    private void testSerializedSize(final TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = File.createTempFile("token-tree-size-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        Assert.assertEquals((int) reader.bytesRemaining(), builder.serializedTokensSize());
        reader.close();
    }

    @Test
    public void buildSerializeAndIterateDynamic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            buildSerializeAndIterate(new DynamicTokenTreeBuilder(tokens), tokens);
    }

    @Test
    public void buildSerializeAndIterateStatic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            buildSerializeAndIterate(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)), tokens);
    }

    private void buildSerializeAndIterate(TokenTreeBuilder builder, SortedMap<Long, KeyOffsets> tokenMap) throws Exception
    {

        builder.finish();
        final File treeFile = File.createTempFile("token-tree-iterate-test1", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            builder.writePartitionDescription(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new MappedBuffer(reader));

        final RangeIterator<Long, Token> tokenIterator = tokenTree.iterator(KeyConverter.instance);
        final Iterator<Map.Entry<Long, KeyOffsets>> listIterator = tokenMap.entrySet().iterator();
        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<Long, KeyOffsets> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        Assert.assertFalse("token iterator not finished", tokenIterator.hasNext());
        Assert.assertFalse("list iterator not finished", listIterator.hasNext());

        tokenIterator.close();
        reader.close();
    }

    @Test
    public void buildSerializeAndGetDynamic() throws Exception
    {
        buildSerializeAndGet(false);
    }

    @Test
    public void buildSerializeAndGetStatic() throws Exception
    {
        buildSerializeAndGet(true);
    }

    public void buildSerializeAndGet(boolean isStatic) throws Exception
    {
        final long tokMin = 0;
        final long tokMax = 1000;

        final TokenTree tokenTree = generateTree(tokMin, tokMax, isStatic);

        for (long i = 0; i <= tokMax; i++)
        {
            TokenTree.OnDiskToken result = tokenTree.get(i, KeyConverter.instance);
            Assert.assertNotNull("failed to find object for token " + i, result);

            KeyOffsets found = result.getOffsets();
            Assert.assertEquals(1, found.partitionCount());
            Assert.assertEquals(i, found.iteratate().iterator().next().partitionPosition);
        }

        Assert.assertNull("found missing object", tokenTree.get(tokMax + 10, KeyConverter.instance));
    }

    @Test
    public void buildSerializeIterateAndSkipDynamic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            buildSerializeIterateAndSkip(new DynamicTokenTreeBuilder(tokens), tokens);
    }

    @Test
    public void buildSerializeIterateAndSkipStatic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            buildSerializeIterateAndSkip(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)), tokens);
    }

    private void buildSerializeIterateAndSkip(TokenTreeBuilder builder, SortedMap<Long, KeyOffsets> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = File.createTempFile("token-tree-iterate-test2", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            builder.writePartitionDescription(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final TokenTree tokenTree = new TokenTree(new MappedBuffer(reader));

        final RangeIterator<Long, Token> treeIterator = tokenTree.iterator(KeyConverter.instance);
        final RangeIterator<Long, TokenWithOffsets> listIterator = new EntrySetSkippableIterator(tokens);

        long lastToken = 0L;
        while (treeIterator.hasNext() && lastToken < 12)
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (lastToken = treeNext.get()));
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));
        }

        treeIterator.skipTo(100548L);
        listIterator.skipTo(100548L);

        while (treeIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = treeIterator.next();
            TokenWithOffsets listNext = listIterator.next();

            Assert.assertEquals(listNext.token, (long) treeNext.get());
            Assert.assertEquals(convert(listNext.offsets), convert(treeNext));

        }

        Assert.assertFalse("Tree iterator not completed", treeIterator.hasNext());
        Assert.assertFalse("List iterator not completed", listIterator.hasNext());

        treeIterator.close();
        reader.close();
    }

    @Test
    public void skipPastEndDynamic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            skipPastEnd(new DynamicTokenTreeBuilder(tokens), tokens);
    }

    @Test
    public void skipPastEndStatic() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            skipPastEnd(new StaticTokenTreeBuilder(new FakeCombinedTerm(tokens)), tokens);
    }

    private void skipPastEnd(TokenTreeBuilder builder, SortedMap<Long, KeyOffsets> tokens) throws Exception
    {
        builder.finish();
        final File treeFile = File.createTempFile("token-tree-skip-past-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            builder.writePartitionDescription(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        final RangeIterator<Long, Token> tokenTree = new TokenTree(new MappedBuffer(reader)).iterator(KeyConverter.instance);

        tokenTree.skipTo(tokens.lastKey() + 10);
        tokenTree.close();
        reader.close();
    }

    @Test
    public void testTokenMergeDyanmic() throws Exception
    {
        testTokenMerge(false);
    }

    @Test
    public void testTokenMergeStatic() throws Exception
    {
        testTokenMerge(true);
    }

    public void testTokenMerge(boolean isStatic) throws Exception
    {
        final long min = 0, max = 1000;

        // two different trees with the same offsets
        TokenTree treeA = generateTree(min, max, isStatic);
        TokenTree treeB = generateTree(min, max, isStatic);

        RangeIterator<Long, Token> a = treeA.iterator(KeyConverter.instance);
        RangeIterator<Long, Token> b = treeB.iterator(KeyConverter.instance);

        long count = min;
        while (a.hasNext() && b.hasNext())
        {
            final Token tokenA = a.next();
            final Token tokenB = b.next();

            // merging of two OnDiskToken
            tokenA.merge(tokenB);
            // merging with RAM Token with different offset
            tokenA.merge(new TokenWithOffsets(tokenA.get(), convert(count + 1)));
            // and RAM token with the same offset
            tokenA.merge(new TokenWithOffsets(tokenA.get(), convert(count)));

            // should fail when trying to merge different tokens
            try
            {
                long l = tokenA.get();
                tokenA.merge(new TokenWithOffsets(l + 1, convert(count)));
                Assert.fail();
            }
            catch (IllegalArgumentException e)
            {
                // expected
            }

            final Set<Long> offsets = new TreeSet<>();
            for (RowKey key : tokenA)
                offsets.add(LongType.instance.compose(key.decoratedKey.getKey()));

            Set<Long> expected = new TreeSet<>();
            {
                expected.add(count);
                expected.add(count + 1);
            }

            Assert.assertEquals(expected, offsets);
            count++;
        }

        Assert.assertEquals(max, count - 1);
        a.close();
        b.close();
    }

    @Test
    public void testEntryTypeOrdinalLookup()
    {
        Assert.assertEquals(EntryType.SIMPLE, EntryType.of(EntryType.SIMPLE.ordinal()));
        Assert.assertEquals(EntryType.PACKED, EntryType.of(EntryType.PACKED.ordinal()));
        Assert.assertEquals(EntryType.FACTORED, EntryType.of(EntryType.FACTORED.ordinal()));
        Assert.assertEquals(EntryType.OVERFLOW, EntryType.of(EntryType.OVERFLOW.ordinal()));
    }

    @Test
    public void testMergingOfEqualTokenTrees() throws Exception
    {
        for (SortedMap<Long, KeyOffsets> tokens : allTokens)
            testMergingOfEqualTokenTrees(tokens);
    }

    private void testMergingOfEqualTokenTrees(SortedMap<Long, KeyOffsets> tokensMap) throws Exception
    {
        TokenTreeBuilder tokensA = new DynamicTokenTreeBuilder(tokensMap);
        TokenTreeBuilder tokensB = new DynamicTokenTreeBuilder(tokensMap);

        Pair<Closeable, TokenTree> a = buildTree(tokensA);
        Pair<Closeable, TokenTree> b = buildTree(tokensB);

        TokenTreeBuilder tokensC = new StaticTokenTreeBuilder(new CombinedTerm(null, null)
        {
            public RangeIterator<Long, Token> getTokenIterator()
            {
                RangeIterator.Builder<Long, Token> union = RangeUnionIterator.builder();
                union.add(a.right.iterator(KeyConverter.instance));
                union.add(b.right.iterator(KeyConverter.instance));

                return union.build();
            }
        });

        Pair<Closeable, TokenTree> c = buildTree(tokensC);
        Assert.assertEquals(tokensMap.size(), c.right.getCount());
        RangeIterator<Long, Token> tokenIterator = c.right.iterator(KeyConverter.instance);
        Iterator<Map.Entry<Long, KeyOffsets>> listIterator = tokensMap.entrySet().iterator();

        while (tokenIterator.hasNext() && listIterator.hasNext())
        {
            Token treeNext = tokenIterator.next();
            Map.Entry<Long, KeyOffsets> listNext = listIterator.next();

            Assert.assertEquals(listNext.getKey(), treeNext.get());
            Assert.assertEquals(convert(listNext.getValue()), convert(treeNext));
        }

        for (Map.Entry<Long, KeyOffsets> entry : tokensMap.entrySet())
        {
            TokenTree.OnDiskToken result = c.right.get(entry.getKey(), KeyConverter.instance);
            Assert.assertNotNull("failed to find object for token " + entry.getKey(), result);
            KeyOffsets found = result.getOffsets();
            Assert.assertEquals(entry.getValue(), found);
        }

        tokenIterator.close();
        a.left.close();
        b.left.close();
        c.left.close();
    }

    private Pair<Closeable, TokenTree> buildTree(TokenTreeBuilder builder) throws Exception
    {
        builder.finish();
        final File treeFile = File.createTempFile("token-tree-", "db");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            builder.writePartitionDescription(writer);
            writer.sync();
        }

        final RandomAccessReader reader = RandomAccessReader.open(treeFile);
        return Pair.create(reader, new TokenTree(new MappedBuffer(reader)));
    }

    private static class EntrySetSkippableIterator extends RangeIterator<Long, TokenWithOffsets>
    {
        private final PeekingIterator<Map.Entry<Long, KeyOffsets>> elements;

        EntrySetSkippableIterator(SortedMap<Long, KeyOffsets> elms)
        {
            super(elms.firstKey(), elms.lastKey(), elms.size());
            elements = Iterators.peekingIterator(elms.entrySet().iterator());
        }

        @Override
        public TokenWithOffsets computeNext()
        {
            if (!elements.hasNext())
                return endOfData();

            Map.Entry<Long, KeyOffsets> next = elements.next();
            return new TokenWithOffsets(next.getKey(), next.getValue());
        }

        @Override
        protected void performSkipTo(Long nextToken)
        {
            while (elements.hasNext())
            {
                if (Long.compare(elements.peek().getKey(), nextToken) >= 0)
                {
                    break;
                }

                elements.next();
            }
        }

        @Override
        public void close() throws IOException
        {
            // nothing to do here
        }
    }

    public static class FakeCombinedTerm extends CombinedTerm
    {
        private final SortedMap<Long, KeyOffsets> tokens;

        public FakeCombinedTerm(SortedMap<Long, KeyOffsets> tokens)
        {
            super(null, null);
            this.tokens = tokens;
        }

        public RangeIterator<Long, Token> getTokenIterator()
        {
            return new TokenMapIterator(tokens);
        }
    }

    public static class TokenMapIterator extends RangeIterator<Long, Token>
    {
        public final Iterator<Map.Entry<Long, KeyOffsets>> iterator;

        public TokenMapIterator(SortedMap<Long, KeyOffsets> tokens)
        {
            super(tokens.firstKey(), tokens.lastKey(), tokens.size());
            iterator = tokens.entrySet().iterator();
        }

        public Token computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Map.Entry<Long, KeyOffsets> entry = iterator.next();
            return new TokenWithOffsets(entry.getKey(), entry.getValue());
        }

        public void close() throws IOException
        {

        }

        public void performSkipTo(Long next)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TokenWithOffsets extends Token
    {
        private final KeyOffsets offsets;

        public TokenWithOffsets(Long token, final KeyOffsets offsets)
        {
            super(token);
            this.offsets = offsets;
        }

        @Override
        public KeyOffsets getOffsets()
        {
            return offsets;
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {}

        @Override
        public int compareTo(CombinedValue<Long> o)
        {
            return Long.compare(token, o.get());
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TokenWithOffsets))
                return false;

            TokenWithOffsets o = (TokenWithOffsets) other;
            return token == o.token && offsets.equals(o.offsets);
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder().append(token).build();
        }

        @Override
        public String toString()
        {
            return String.format("TokenValue(token: %d, offsets: %s)", token, offsets);
        }

        @Override
        public Iterator<RowKey> iterator()
        {
            return offsets.getKeyIterator(KeyConverter.instance);
//            List<RowKey> keys = new ArrayList<>(offsets.partitionCount());
//            for (LongObjectCursor<LongArrayList> offset : offsets)
//                for (LongCursor l : offset.value)
//                {
//                    keys.add(KeyConverter.instance.getRowKey(KeyConverter.dk(offset.key), l.value));
//                }
//            return keys.iterator();
        }
    }

    private static Set<RowKey> convert(KeyOffsets offsets)
    {
        Set<RowKey> keys = new HashSet<>();
        Iterator<RowKey> iter = offsets.getKeyIterator(KeyConverter.instance);
        while (iter.hasNext())
        {
            keys.add(iter.next());
        }
//        for (LongObjectCursor<LongArrayList> offset : offsets)
//            for (LongCursor l : offset.value)
//                keys.add(new RowKey(KeyConverter.dk(offset.key),
//                                    KeyConverter.ck(l.value),
//                                    CLUSTERING_COMPARATOR));

        return keys;
    }

    private static Set<RowKey> convert(Token results)
    {
        Set<RowKey> keys = new HashSet<>();
        for (RowKey key : results)
            keys.add(key);

        return keys;
    }

    private static KeyOffsets convert(long... values)
    {
        KeyOffsets result = new KeyOffsets();
        for (long v : values)
            result.put(v, KeyOffsets.asArray(v + 5));

        return result;
    }

    private TokenTree generateTree(final long minToken, final long maxToken, boolean isStatic) throws IOException
    {
        final SortedMap<Long, KeyOffsets> toks = new TreeMap<Long, KeyOffsets>()
        {{
            for (long i = minToken; i <= maxToken; i++)
            {
                KeyOffsets offsetSet = new KeyOffsets();
                offsetSet.put(i, KeyOffsets.asArray(i + 5));
                put(i, offsetSet);
            }
        }};

        final TokenTreeBuilder builder = isStatic ? new StaticTokenTreeBuilder(new FakeCombinedTerm(toks)) : new DynamicTokenTreeBuilder(toks);
        builder.finish();
        final File treeFile = File.createTempFile("token-tree-get-test", "tt");
        treeFile.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(treeFile, DEFAULT_OPT))
        {
            builder.writeTokens(writer);
            builder.writePartitionDescription(writer);
            writer.sync();
        }

        RandomAccessReader reader = null;

        try
        {
            reader = RandomAccessReader.open(treeFile);
            return new TokenTree(new MappedBuffer(reader));
        }
        finally
        {
            FileUtils.closeQuietly(reader);
        }
    }
}