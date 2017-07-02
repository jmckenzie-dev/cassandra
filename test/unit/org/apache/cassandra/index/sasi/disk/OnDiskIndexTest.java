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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.CombinedTermIterator;
import org.apache.cassandra.index.sasi.utils.KeyConverter;
import org.apache.cassandra.index.sasi.utils.OnDiskIndexIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.TOKENS_PER_BLOCK;

public class OnDiskIndexTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testStringSAConstruction() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(UTF8Type.instance.decompose("scat"), keyBuilder(1L));
                put(UTF8Type.instance.decompose("mat"),  keyBuilder(2L));
                put(UTF8Type.instance.decompose("fat"),  keyBuilder(3L));
                put(UTF8Type.instance.decompose("cat"),  keyBuilder(1L, 4L));
                put(UTF8Type.instance.decompose("till"), keyBuilder(2L, 6L));
                put(UTF8Type.instance.decompose("bill"), keyBuilder(5L));
                put(UTF8Type.instance.decompose("foo"),  keyBuilder(7L));
                put(UTF8Type.instance.decompose("bar"),  keyBuilder(9L, 10L));
                put(UTF8Type.instance.decompose("michael"), keyBuilder(11L, 12L, 1L));
                put(UTF8Type.instance.decompose("am"), keyBuilder(15L));
        }};

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, UTF8Type.instance, OnDiskIndexBuilder.Mode.CONTAINS);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            addAll(builder, e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-string", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, UTF8Type.instance, KeyConverter.instance);

        // first check if we can find exact matches
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
        {
            if (UTF8Type.instance.getString(e.getKey()).equals("cat"))
                continue; // cat is embedded into scat, we'll test it in next section

            Assert.assertEquals("Key was: " + UTF8Type.instance.compose(e.getKey()),
                                convert(e.getValue()),
                                convert(onDisk.search(expressionFor(UTF8Type.instance, e.getKey()))));
        }

        // check that cat returns positions for scat & cat
        Assert.assertEquals(convert(1L, 4L), convert(onDisk.search(expressionFor("cat"))));

        // random suffix queries
        Assert.assertEquals(convert(9, 10), convert(onDisk.search(expressionFor("ar"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(expressionFor("at"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(expressionFor("mic"))));
        Assert.assertEquals(convert(1, 11, 12), convert(onDisk.search(expressionFor("ae"))));
        Assert.assertEquals(convert(2, 5, 6), convert(onDisk.search(expressionFor("ll"))));
        Assert.assertEquals(convert(1, 2, 5, 6, 11, 12), convert(onDisk.search(expressionFor("l"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(expressionFor("oo"))));
        Assert.assertEquals(convert(7), convert(onDisk.search(expressionFor("o"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 6), convert(onDisk.search(expressionFor("t"))));
        Assert.assertEquals(convert(1, 2, 11, 12), convert(onDisk.search(expressionFor("m", Operator.LIKE_PREFIX))));

        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("hello"))));

        onDisk.close();
    }

    @Test
    public void testIntegerSAConstruction() throws Exception
    {
        final Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(Int32Type.instance.decompose(5),  keyBuilder(1L));
                put(Int32Type.instance.decompose(7),  keyBuilder(2L));
                put(Int32Type.instance.decompose(1),  keyBuilder(3L));
                put(Int32Type.instance.decompose(3),  keyBuilder(1L, 4L));
                put(Int32Type.instance.decompose(8),  keyBuilder(2L, 6L));
                put(Int32Type.instance.decompose(10), keyBuilder(5L));
                put(Int32Type.instance.decompose(6),  keyBuilder(7L));
                put(Int32Type.instance.decompose(4),  keyBuilder(9L, 10L));
                put(Int32Type.instance.decompose(0),  keyBuilder(11L, 12L, 1L));
        }};

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, Int32Type.instance, OnDiskIndexBuilder.Mode.PREFIX);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            addAll(builder, e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-int", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, Int32Type.instance, KeyConverter.instance);

        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
        {
            Assert.assertEquals(convert(e.getValue()), convert(onDisk.search(expressionFor(Operator.EQ, Int32Type.instance, e.getKey()))));
        }

        List<ByteBuffer> sortedNumbers = new ArrayList<ByteBuffer>()
        {{
            addAll(data.keySet().stream().collect(Collectors.toList()));
        }};

        Collections.sort(sortedNumbers, Int32Type.instance::compare);

        // test full iteration
        int idx = 0;
        for (OnDiskIndex.DataTerm term : onDisk)
        {
            ByteBuffer number = sortedNumbers.get(idx++);
            Assert.assertEquals(number, term.getTerm());
            Assert.assertEquals(convert(data.get(number)), convert(term.getTokens()));
        }

        // test partial iteration (descending)
        idx = 3; // start from the 3rd element
        Iterator<OnDiskIndex.DataTerm> partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskIndex.IteratorOrder.DESC, true);
        while (partialIter.hasNext())
        {
            OnDiskIndex.DataTerm term = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, term.getTerm());
            Assert.assertEquals(convert(data.get(number)), convert(term.getTokens()));
        }

        idx = 3; // start from the 3rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx++), OnDiskIndex.IteratorOrder.DESC, false);
        while (partialIter.hasNext())
        {
            OnDiskIndex.DataTerm term = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx++);

            Assert.assertEquals(number, term.getTerm());
            Assert.assertEquals(convert(data.get(number)), convert(term.getTokens()));
        }

        // test partial iteration (ascending)
        idx = 6; // start from the 6rd element
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx), OnDiskIndex.IteratorOrder.ASC, true);
        while (partialIter.hasNext())
        {
            OnDiskIndex.DataTerm term = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, term.getTerm());
            Assert.assertEquals(convert(data.get(number)), convert(term.getTokens()));
        }

        idx = 6; // start from the 6rd element exclusive
        partialIter = onDisk.iteratorAt(sortedNumbers.get(idx--), OnDiskIndex.IteratorOrder.ASC, false);
        while (partialIter.hasNext())
        {
            OnDiskIndex.DataTerm term = partialIter.next();
            ByteBuffer number = sortedNumbers.get(idx--);

            Assert.assertEquals(number, term.getTerm());
            Assert.assertEquals(convert(data.get(number)), convert(term.getTokens()));
        }

        onDisk.close();

        List<ByteBuffer> iterCheckNums = new ArrayList<ByteBuffer>()
        {{
            add(Int32Type.instance.decompose(3));
            add(Int32Type.instance.decompose(9));
            add(Int32Type.instance.decompose(14));
            add(Int32Type.instance.decompose(42));
        }};

        OnDiskIndexBuilder iterTest = new OnDiskIndexBuilder(UTF8Type.instance, Int32Type.instance, OnDiskIndexBuilder.Mode.PREFIX);
        for (int i = 0; i < iterCheckNums.size(); i++)
            iterTest .add(iterCheckNums.get(i), keyAt((long) i), i, i + 5);

        File iterIndex = File.createTempFile("sa-iter", ".db");
        iterIndex.deleteOnExit();

        iterTest.finish(iterIndex);

        onDisk = new OnDiskIndex(iterIndex, Int32Type.instance, KeyConverter.instance);

        ByteBuffer number = Int32Type.instance.decompose(1);
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, false)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, true)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, false)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(44);
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, false)));
        Assert.assertEquals(4, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, true)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, false)));
        Assert.assertEquals(0, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(20);
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, false)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, true)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, false)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(5);
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, false)));
        Assert.assertEquals(1, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, true)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, false)));
        Assert.assertEquals(3, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, true)));

        number = Int32Type.instance.decompose(10);
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, false)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.ASC, true)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, false)));
        Assert.assertEquals(2, Iterators.size(onDisk.iteratorAt(number, OnDiskIndex.IteratorOrder.DESC, true)));

        onDisk.close();
    }

    @Test
    public void testMultiSuffixMatches() throws Exception
    {
        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, UTF8Type.instance, OnDiskIndexBuilder.Mode.CONTAINS)
        {{
                addAll(this, UTF8Type.instance.decompose("Eliza"), keyBuilder(1L, 2L));
                addAll(this, UTF8Type.instance.decompose("Elizabeth"), keyBuilder(3L, 4L));
                addAll(this, UTF8Type.instance.decompose("Aliza"), keyBuilder(5L, 6L));
                addAll(this, UTF8Type.instance.decompose("Taylor"), keyBuilder(7L, 8L));
                addAll(this, UTF8Type.instance.decompose("Pavel"), keyBuilder(9L, 10L));
        }};

        File index = File.createTempFile("on-disk-sa-multi-suffix-match", ".db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, UTF8Type.instance, KeyConverter.instance);

        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(expressionFor("liz"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionFor("a"))));
        Assert.assertEquals(convert(5, 6), convert(onDisk.search(expressionFor("A"))));
        Assert.assertEquals(convert(1, 2, 3, 4), convert(onDisk.search(expressionFor("E"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionFor("l"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(expressionFor("bet"))));
        Assert.assertEquals(convert(3, 4, 9, 10), convert(onDisk.search(expressionFor("e"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(expressionFor("yl"))));
        Assert.assertEquals(convert(7, 8), convert(onDisk.search(expressionFor("T"))));
        Assert.assertEquals(convert(1, 2, 3, 4, 5, 6), convert(onDisk.search(expressionFor("za"))));
        Assert.assertEquals(convert(3, 4), convert(onDisk.search(expressionFor("ab"))));

        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Pi"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("ethz"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("liw"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Taw"))));
        Assert.assertEquals(Collections.<DecoratedKey>emptySet(), convert(onDisk.search(expressionFor("Av"))));

        onDisk.close();
    }

    @Test
    public void testSparseMode() throws Exception
    {
        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, LongType.instance, OnDiskIndexBuilder.Mode.SPARSE);

        final long start = System.currentTimeMillis();
        final int numIterations = 100000;

        System.out.println("TOKENS_PER_BLOCK  = " + TOKENS_PER_BLOCK);
        for (long i = 0; i < numIterations; i++)
            builder.add(LongType.instance.decompose(start + i), keyAt(i), i, clusteringOffset(i));

        File index = File.createTempFile("on-disk-sa-sparse", "db");
        System.out.println("index = " + index);
//        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, LongType.instance, KeyConverter.instance);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        for (long step = start; step < (start + numIterations); step += 1000)
        {
            boolean lowerInclusive = random.nextBoolean();
            boolean upperInclusive = random.nextBoolean();

            long limit = random.nextLong(step, start + numIterations);
            RangeIterator<Long, Token> rows = onDisk.search(expressionFor(step, lowerInclusive, limit, upperInclusive));

            long lowerKey = step - start;
            long upperKey = lowerKey + (limit - step);

            if (!lowerInclusive)
                lowerKey += 1;

            if (upperInclusive)
                upperKey += 1;

            Set<RowKey> actual = convert(rows);
            for (long key = lowerKey; key < upperKey; key++)
                Assert.assertTrue("key " + key + " wasn't found", actual.contains(new RowKey(keyAt(key), ck(clusteringOffset(key)), CLUSTERING_COMPARATOR)));

            Assert.assertEquals((upperKey - lowerKey), actual.size());
        }

        // let's also explicitly test whole range search
        RangeIterator<Long, Token> rows = onDisk.search(expressionFor(start, true, start + numIterations, true));

        Set<RowKey> actual = convert(rows);
        Assert.assertEquals(numIterations, actual.size());
    }

    @Test
    public void testNotEqualsQueryForStrings() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(UTF8Type.instance.decompose("Pavel"),   keyBuilder(1L, 2L));
                put(UTF8Type.instance.decompose("Jason"),   keyBuilder(3L));
                put(UTF8Type.instance.decompose("Jordan"),  keyBuilder(4L));
                put(UTF8Type.instance.decompose("Michael"), keyBuilder(5L, 6L));
                put(UTF8Type.instance.decompose("Vijay"),   keyBuilder(7L));
                put(UTF8Type.instance.decompose("Travis"),  keyBuilder(8L));
                put(UTF8Type.instance.decompose("Aleksey"), keyBuilder(9L, 10L));
        }};

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, UTF8Type.instance, OnDiskIndexBuilder.Mode.PREFIX);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            addAll(builder, e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-except-test", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, UTF8Type.instance, KeyConverter.instance);

        // test whole words first
        Assert.assertEquals(convert(3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pavel"))));

        Assert.assertEquals(convert(3, 4, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pavel", "Michael"))));

        Assert.assertEquals(convert(3, 4, 7, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pavel", "Michael", "Travis"))));

        // now test prefixes
        Assert.assertEquals(convert(3, 4, 5, 6, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pav"))));

        Assert.assertEquals(convert(3, 4, 7, 8, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pavel", "Mic"))));

        Assert.assertEquals(convert(3, 4, 7, 9, 10), convert(onDisk.search(expressionForNot("Aleksey", "Vijay", "Pavel", "Micha", "Tr"))));

        onDisk.close();
    }

    @Test
    public void testNotEqualsQueryForNumbers() throws Exception
    {
        final Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{
                put(Int32Type.instance.decompose(5),  keyBuilder(1L));
                put(Int32Type.instance.decompose(7),  keyBuilder(2L));
                put(Int32Type.instance.decompose(1),  keyBuilder(3L));
                put(Int32Type.instance.decompose(3),  keyBuilder(1L, 4L));
                put(Int32Type.instance.decompose(8),  keyBuilder(8L, 6L));
                put(Int32Type.instance.decompose(10), keyBuilder(5L));
                put(Int32Type.instance.decompose(6),  keyBuilder(7L));
                put(Int32Type.instance.decompose(4),  keyBuilder(9L, 10L));
                put(Int32Type.instance.decompose(0),  keyBuilder(11L, 12L, 1L));
        }};

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, Int32Type.instance, OnDiskIndexBuilder.Mode.PREFIX);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            addAll(builder, e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-except-int-test", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, Int32Type.instance, KeyConverter.instance);

        Assert.assertEquals(convert(1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12), convert(onDisk.search(expressionForNot(0, 10, 1))));
        Assert.assertEquals(convert(1, 2, 4, 5, 7, 9, 10, 11, 12), convert(onDisk.search(expressionForNot(0, 10, 1, 8))));
        Assert.assertEquals(convert(1, 2, 4, 5, 7, 11, 12), convert(onDisk.search(expressionForNot(0, 10, 1, 8, 4))));

        onDisk.close();
    }

    @Test
    public void testRangeQueryWithExclusions() throws Exception
    {
        final long lower = 0;
        final long upper = 100000;

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(LongType.instance, LongType.instance, OnDiskIndexBuilder.Mode.SPARSE);
        for (long i = lower; i <= upper; i++)
            builder.add(LongType.instance.decompose(i), keyAt(i), i, clusteringOffset(i));

        File index = File.createTempFile("on-disk-sa-except-long-ranges", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, LongType.instance, KeyConverter.instance);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // single exclusion

        // let's do small range first to figure out if searchPoint works properly
        validateExclusions(onDisk, lower, 50, Sets.newHashSet(42L));
        // now let's do whole data set to test SPARSE searching
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L));

        // pair of exclusions which would generate a split

        validateExclusions(onDisk, lower, random.nextInt(400, 800), Sets.newHashSet(42L, 154L));
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L, 54631L));

        // 3 exclusions which would generate a split and change bounds

        validateExclusions(onDisk, lower, random.nextInt(400, 800), Sets.newHashSet(42L, 154L));
        validateExclusions(onDisk, lower, upper, Sets.newHashSet(31337L, 54631L));

        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L));
        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L, 93L));
        validateExclusions(onDisk, lower, random.nextLong(400, upper), Sets.newHashSet(42L, 55L, 93L, 205L));

        Set<Long> exclusions = Sets.newHashSet(3L, 12L, 13L, 14L, 27L, 54L, 81L, 125L, 384L, 771L, 1054L, 2048L, 78834L);

        // test that exclusions are properly bound by lower/upper of the expression
        Assert.assertEquals(392, validateExclusions(onDisk, lower, 400, exclusions, false));
        Assert.assertEquals(101, validateExclusions(onDisk, lower, 100, Sets.newHashSet(-10L, -5L, -1L), false));

        validateExclusions(onDisk, lower, upper, exclusions);

        Assert.assertEquals(100000, convert(onDisk.search(new Expression("", LongType.instance)
                                                    .add(Operator.NEQ, LongType.instance.decompose(100L)))).size());

        Assert.assertEquals(49, convert(onDisk.search(new Expression("", LongType.instance)
                                                    .add(Operator.LT, LongType.instance.decompose(50L))
                                                    .add(Operator.NEQ, LongType.instance.decompose(10L)))).size());

        Assert.assertEquals(99998, convert(onDisk.search(new Expression("", LongType.instance)
                                                    .add(Operator.GT, LongType.instance.decompose(1L))
                                                    .add(Operator.NEQ, LongType.instance.decompose(20L)))).size());

        onDisk.close();
    }

    private void validateExclusions(OnDiskIndex sa, long lower, long upper, Set<Long> exclusions)
    {
        validateExclusions(sa, lower, upper, exclusions, true);
    }

    private int validateExclusions(OnDiskIndex sa, long lower, long upper, Set<Long> exclusions, boolean checkCount)
    {
        int count = 0;
        for (RowKey key : convert(sa.search(rangeWithExclusions(lower, true, upper, true, exclusions))))
        {
            long keyId = LongType.instance.compose(key.decoratedKey.getKey());
            Assert.assertFalse("key" + keyId + " is present.", exclusions.contains(keyId));
            count++;
        }

        if (checkCount)
            Assert.assertEquals(upper - (lower == 0 ? -1 : lower) - exclusions.size(), count);

        return count;
    }

    @Test
    public void testDescriptor() throws Exception
    {
        final Map<ByteBuffer, Pair<RowKey, Long>> data = new HashMap<ByteBuffer, Pair<RowKey, Long>>()
        {{
                put(Int32Type.instance.decompose(5), Pair.create(new RowKey(keyAt(1L), ck(clusteringOffset(1L)), CLUSTERING_COMPARATOR) , 1L));
        }};

        OnDiskIndexBuilder builder1 = new OnDiskIndexBuilder(UTF8Type.instance, Int32Type.instance, OnDiskIndexBuilder.Mode.PREFIX);
        for (Map.Entry<ByteBuffer, Pair<RowKey, Long>> e : data.entrySet())
        {
            DecoratedKey key = e.getValue().left.decoratedKey;
            Long position = e.getValue().right;

            builder1.add(e.getKey(), key, position, clusteringOffset(position));
        }

        File index1 = File.createTempFile("on-disk-sa-int", "db");

        index1.deleteOnExit();

        builder1.finish(index1);
        OnDiskIndex onDisk1 = new OnDiskIndex(index1, Int32Type.instance, KeyConverter.instance);
        ByteBuffer number = Int32Type.instance.decompose(5);
        Assert.assertEquals(Collections.singleton(data.get(number).left), convert(onDisk1.search(expressionFor(Operator.EQ, Int32Type.instance, number))));
        Assert.assertEquals(onDisk1.descriptor.version, Descriptor.CURRENT_VERSION);
    }


    static final String DATA_DIR = "test/data/legacy-sasi/";

    @Test
    public void testLegacyDescriptor() throws Exception
    {
        final Map<ByteBuffer, Pair<RowKey, Long>> data = new HashMap<ByteBuffer, Pair<RowKey, Long>>()
        {{
            put(Int32Type.instance.decompose(5), Pair.create(new RowKey(keyAt(1L), ck(KeyOffsets.NO_OFFSET), CLUSTERING_COMPARATOR) , 1L));
        }};

        File index2 = new File(DATA_DIR + "on-disk-sa-int2.db");
        OnDiskIndex onDisk2 = new OnDiskIndex(index2, Int32Type.instance, KeyConverter.instance);

        ByteBuffer number = Int32Type.instance.decompose(5);
        Assert.assertEquals(Collections.singleton(data.get(number).left), convert(onDisk2.search(expressionFor(Operator.EQ, Int32Type.instance, number))));

        Assert.assertEquals(onDisk2.descriptor.version, Descriptor.VERSION_AA);
    }

    @Test
    public void testSuperBlocks() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> terms = new HashMap<>();
        terms.put(UTF8Type.instance.decompose("1234"), keyBuilder(1L, 2L));
        terms.put(UTF8Type.instance.decompose("2345"), keyBuilder(3L, 4L));
        terms.put(UTF8Type.instance.decompose("3456"), keyBuilder(5L, 6L));
        terms.put(UTF8Type.instance.decompose("4567"), keyBuilder(7L, 8L));
        terms.put(UTF8Type.instance.decompose("5678"), keyBuilder(9L, 10L));

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, Int32Type.instance, OnDiskIndexBuilder.Mode.SPARSE);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> entry : terms.entrySet())
            addAll(builder, entry.getKey(), entry.getValue());

        File index = File.createTempFile("on-disk-sa-try-superblocks", ".db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, Int32Type.instance, KeyConverter.instance);
        OnDiskIndex.OnDiskSuperBlock superBlock = onDisk.dataLevel.getSuperBlock(0);
        Iterator<Token> iter = superBlock.iterator();

        Long lastToken = null;
        while (iter.hasNext())
        {
            Token token = iter.next();

            if (lastToken != null)
                Assert.assertTrue(lastToken.compareTo(token.get()) < 0);

            lastToken = token.get();
        }
    }

    @Test
    public void testSuperBlockRetrieval() throws Exception
    {
        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, LongType.instance, OnDiskIndexBuilder.Mode.SPARSE);
        for (long i = 0; i < 100000; i++)
            builder.add(LongType.instance.decompose(i), keyAt(i), i, clusteringOffset(i));

        File index = File.createTempFile("on-disk-sa-multi-superblock-match", ".db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDiskIndex = new OnDiskIndex(index, LongType.instance, KeyConverter.instance);

        testSearchRangeWithSuperBlocks(onDiskIndex, 0, 500);
        testSearchRangeWithSuperBlocks(onDiskIndex, 300, 93456);
        testSearchRangeWithSuperBlocks(onDiskIndex, 210, 1700);
        testSearchRangeWithSuperBlocks(onDiskIndex, 530, 3200);

        Random random = new Random(0xdeadbeef);
        for (int i = 0; i < 100000; i += random.nextInt(1500)) // random steps with max of 1500 elements
        {
            for (int j = 0; j < 3; j++)
                testSearchRangeWithSuperBlocks(onDiskIndex, i, ThreadLocalRandom.current().nextInt(i, 100000));
        }
    }

    public void putAll(SortedMap<Long, KeyOffsets> offsets, TokenTreeBuilder ttb)
    {
        for (Pair<Long, KeyOffsets> entry : ttb)
            offsets.put(entry.left, entry.right);
    }

    @Test
    public void testCombiningOfThePartitionedSA() throws Exception
    {
        OnDiskIndexBuilder builderA = new OnDiskIndexBuilder(UTF8Type.instance, LongType.instance, OnDiskIndexBuilder.Mode.PREFIX);
        OnDiskIndexBuilder builderB = new OnDiskIndexBuilder(UTF8Type.instance, LongType.instance, OnDiskIndexBuilder.Mode.PREFIX);

        TreeMap<Long, TreeMap<Long, KeyOffsets>> expected = new TreeMap<>();

        for (long i = 0; i <= 100; i++)
        {
            TreeMap<Long, KeyOffsets> offsets = expected.get(i);
            if (offsets == null)
                expected.put(i, (offsets = new TreeMap<>()));

            builderA.add(LongType.instance.decompose(i), keyAt(i), i, clusteringOffset(i));
            putAll(offsets, keyBuilder(i));
        }

        for (long i = 50; i < 100; i++)
        {
            TreeMap<Long, KeyOffsets> offsets = expected.get(i);
            if (offsets == null)
                expected.put(i, (offsets = new TreeMap<>()));

            long position = 100L + i;
            builderB.add(LongType.instance.decompose(i), keyAt(position), position, clusteringOffset(position));
            putAll(offsets, keyBuilder(100L + i));
        }

        File indexA = File.createTempFile("on-disk-sa-partition-a", ".db");
        indexA.deleteOnExit();

        File indexB = File.createTempFile("on-disk-sa-partition-b", ".db");
        indexB.deleteOnExit();

        builderA.finish(indexA);
        builderB.finish(indexB);

        OnDiskIndex a = new OnDiskIndex(indexA, LongType.instance, KeyConverter.instance);
        OnDiskIndex b = new OnDiskIndex(indexB, LongType.instance, KeyConverter.instance);

        RangeIterator<OnDiskIndex.DataTerm, CombinedTerm> union = OnDiskIndexIterator.union(a, b);

        TreeMap<Long, TreeMap<Long, KeyOffsets>> actual = new TreeMap<>();
        while (union.hasNext())
        {
            CombinedTerm term = union.next();

            Long composedTerm = LongType.instance.compose(term.getTerm());

            TreeMap<Long, KeyOffsets> offsets = actual.get(composedTerm);
            if (offsets == null)
                actual.put(composedTerm, (offsets = new TreeMap<>()));

            putAll(offsets, term.getTokenTreeBuilder());
        }

        Assert.assertEquals(actual, expected);

        File indexC = File.createTempFile("on-disk-sa-partition-final", ".db");
        indexC.deleteOnExit();

        OnDiskIndexBuilder combined = new OnDiskIndexBuilder(UTF8Type.instance, LongType.instance, OnDiskIndexBuilder.Mode.PREFIX);
        combined.finish(Pair.create(keyAt(0).getKey(), keyAt(100).getKey()), indexC, new CombinedTermIterator(a, b));

        OnDiskIndex c = new OnDiskIndex(indexC, LongType.instance, KeyConverter.instance);
        union = OnDiskIndexIterator.union(c);
        actual.clear();

        while (union.hasNext())
        {
            CombinedTerm term = union.next();

            Long composedTerm = LongType.instance.compose(term.getTerm());

            TreeMap<Long, KeyOffsets> offsets = actual.get(composedTerm);
            if (offsets == null)
                actual.put(composedTerm, (offsets = new TreeMap<>()));

            putAll(offsets, term.getTokenTreeBuilder());
        }

        Assert.assertEquals(actual, expected);

        a.close();
        b.close();
    }

    @Test
    public void testPrefixSearchWithCONTAINSMode() throws Exception
    {
        Map<ByteBuffer, TokenTreeBuilder> data = new HashMap<ByteBuffer, TokenTreeBuilder>()
        {{

            put(UTF8Type.instance.decompose("lady gaga"), keyBuilder(1L));

            // Partial term for 'lady of bells'
            DataOutputBuffer ladyOfBellsBuffer = new DataOutputBuffer();
            ladyOfBellsBuffer.writeShort(UTF8Type.instance.decompose("lady of bells").remaining() | (1 << OnDiskIndexBuilder.IS_PARTIAL_BIT));
            ladyOfBellsBuffer.write(UTF8Type.instance.decompose("lady of bells"));
            put(ladyOfBellsBuffer.asNewBuffer(), keyBuilder(2L));


            put(UTF8Type.instance.decompose("lady pank"),  keyBuilder(3L));
        }};

        OnDiskIndexBuilder builder = new OnDiskIndexBuilder(UTF8Type.instance, UTF8Type.instance, OnDiskIndexBuilder.Mode.CONTAINS);
        for (Map.Entry<ByteBuffer, TokenTreeBuilder> e : data.entrySet())
            addAll(builder, e.getKey(), e.getValue());

        File index = File.createTempFile("on-disk-sa-prefix-contains-search", "db");
        index.deleteOnExit();

        builder.finish(index);

        OnDiskIndex onDisk = new OnDiskIndex(index, UTF8Type.instance, KeyConverter.instance);

        // check that lady% return lady gaga (1) and lady pank (3) but not lady of bells(2)
        Assert.assertEquals(convert(1, 3), convert(onDisk.search(expressionFor("lady", Operator.LIKE_PREFIX))));

        onDisk.close();
    }

    private void testSearchRangeWithSuperBlocks(OnDiskIndex onDiskIndex, long start, long end)
    {
        RangeIterator<Long, Token> tokens = onDiskIndex.search(expressionFor(start, true, end, false));

        // no results should be produced only if range is empty
        if (tokens == null)
        {
            Assert.assertEquals(0, end - start);
            return;
        }

        int keyCount = 0;
        Long lastToken = null;
        while (tokens.hasNext())
        {
            Token token = tokens.next();
            Iterator<RowKey> keys = token.iterator();

            // each of the values should have exactly a single key
            Assert.assertTrue(keys.hasNext());
            keys.next();
            Assert.assertFalse(keys.hasNext());

            // and it's last should always smaller than current
            if (lastToken != null)
                Assert.assertTrue("last should be less than current", lastToken < token.get());

            lastToken = token.get();
            keyCount++;
        }

        Assert.assertEquals(end - start, keyCount);
    }

    private static DecoratedKey keyAt(long partitionOffset)
    {
        return KeyConverter.dk(partitionOffset);
    }

    private static Clustering ck(long rowOffset)
    {
        return KeyConverter.ck(rowOffset);
    }

    private TokenTreeBuilder keyBuilder(long... offsets)
    {
        TokenTreeBuilder builder = new DynamicTokenTreeBuilder();

        for (final long pkOffset : offsets)
        {
            DecoratedKey k = keyAt(pkOffset);
            builder.add((Long) k.getToken().getTokenValue(), pkOffset, clusteringOffset(pkOffset));
        }

        return builder.finish();
    }

    private static long clusteringOffset(long offset)
    {
        return offset + 100;
    }

    private TokenTreeBuilder keyBuilder(Pair<Long, Long>... offsets)
    {
        TokenTreeBuilder builder = new DynamicTokenTreeBuilder();

        for (final Pair<Long,Long> key : offsets)
        {
            DecoratedKey k = keyAt(key.left);
            builder.add((Long) k.getToken().getTokenValue(), key.left, key.right);
        }

        return builder.finish();
    }

    private static final ClusteringComparator CLUSTERING_COMPARATOR = new ClusteringComparator(BytesType.instance);

    private static Set<RowKey> convert(TokenTreeBuilder offsets)
    {
        Set<RowKey> result = new HashSet<>();

        Iterator<Pair<Long, KeyOffsets>> offsetIter = offsets.iterator();
        while (offsetIter.hasNext())
        {
            Pair<Long, KeyOffsets> pair = offsetIter.next();

            Iterator<RowKey> iter = pair.right.getKeyIterator(KeyConverter.instance);
            while (iter.hasNext())
            {
                result.add(iter.next());
            }
//            for (LongObjectCursor<LongArrayList> cursor : pair.right)
//                for (LongCursor l : cursor.value)
//                    result.add(new RowKey(keyAt(cursor.key), ck(l.value), CLUSTERING_COMPARATOR));
        }
        return result;
    }

    private static Set<RowKey> convert(long... keyOffsets)
    {
        Set<RowKey> result = new HashSet<>();
        for (final long offset : keyOffsets)
            result.add(new RowKey(keyAt(offset), ck(clusteringOffset(offset)), CLUSTERING_COMPARATOR));

        return result;
    }

    private static Set<RowKey> convert(RangeIterator<Long, Token> results)
    {
        if (results == null)
            return Collections.emptySet();

        Set<RowKey> keys = new TreeSet<>();

        int i = 0;
        while (results.hasNext()) {
            for (RowKey key: results.next())
            {
//                if (keys.contains(key))
//                    System.out.println("key = " + key);
//
                keys.add(key);
                i++;
            }
        }

//        System.out.println("i = " + i);
        return keys;
    }

    private static Expression expressionFor(long lower, boolean lowerInclusive, long upper, boolean upperInclusive)
    {
        Expression expression = new Expression("", LongType.instance);
        expression.add(lowerInclusive ? Operator.GTE : Operator.GT, LongType.instance.decompose(lower));
        expression.add(upperInclusive ? Operator.LTE : Operator.LT, LongType.instance.decompose(upper));
        return expression;
    }

    private static Expression expressionFor(AbstractType<?> validator, ByteBuffer term)
    {
        return expressionFor(Operator.LIKE_CONTAINS, validator, term);
    }

    private static Expression expressionFor(Operator op, AbstractType<?> validator, ByteBuffer term)
    {
        Expression expression = new Expression("", validator);
        expression.add(op, term);
        return expression;
    }

    private static Expression expressionForNot(AbstractType<?> validator, ByteBuffer lower, ByteBuffer upper, Iterable<ByteBuffer> terms)
    {
        Expression expression = new Expression("", validator);
        expression.setOp(Expression.Op.RANGE);
        expression.setLower(new Expression.Bound(lower, true));
        expression.setUpper(new Expression.Bound(upper, true));
        for (ByteBuffer term : terms)
            expression.add(Operator.NEQ, term);
        return expression;

    }

    private static Expression expressionForNot(Integer lower, Integer upper, Integer... terms)
    {
        return expressionForNot(Int32Type.instance,
                Int32Type.instance.decompose(lower),
                Int32Type.instance.decompose(upper),
                Arrays.asList(terms).stream().map(Int32Type.instance::decompose).collect(Collectors.toList()));
    }

    private static Expression rangeWithExclusions(long lower, boolean lowerInclusive, long upper, boolean upperInclusive, Set<Long> exclusions)
    {
        Expression expression = expressionFor(lower, lowerInclusive, upper, upperInclusive);
        for (long e : exclusions)
            expression.add(Operator.NEQ, LongType.instance.decompose(e));

        return expression;
    }

    private static Expression expressionForNot(String lower, String upper, String... terms)
    {
        return expressionForNot(UTF8Type.instance,
                UTF8Type.instance.decompose(lower),
                UTF8Type.instance.decompose(upper),
                Arrays.asList(terms).stream().map(UTF8Type.instance::decompose).collect(Collectors.toList()));
    }

    private static Expression expressionFor(String term)
    {
        return expressionFor(term, Operator.LIKE_CONTAINS);
    }

    private static Expression expressionFor(String term, Operator op)
    {
        return expressionFor(op, UTF8Type.instance, UTF8Type.instance.decompose(term));
    }

    private static void addAll(OnDiskIndexBuilder builder, ByteBuffer term, TokenTreeBuilder tokens) throws IOException
    {
        for (Pair<Long, KeyOffsets> token : tokens)
        {
            token.right.iteratate(new KeyOffsets.KeyOffsetIterator()
            {
                private DecoratedKey decoratedKey;
                private long offset;
                public void onPartition(long partitionPosition, int rowCount)
                {
                    decoratedKey = keyAt(partitionPosition);
                    offset = partitionPosition;
                }

                public void onRow(long rowPosition)
                {
                    builder.add(term, decoratedKey, offset, rowPosition);
                }
            });
        }
    }
}
