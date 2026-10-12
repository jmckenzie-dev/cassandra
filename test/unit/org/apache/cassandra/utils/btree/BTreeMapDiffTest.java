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

package org.apache.cassandra.utils.btree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link BTreeMap#diff} must report exactly the entries that differ, and visit only what changed when the maps share
 * structure: schema diffs on every DDL depend on it.
 */
public class BTreeMapDiffTest
{
    private static final int[] SIZES = { 0, 1, 2, 15, 31, 32, 33, 100, 1000, 5000 };

    @Test
    public void randomEditsReportExactlyTheDifferences()
    {
        Random random = new Random(42);
        for (int size : SIZES)
        {
            for (int round = 0; round < 20; round++)
            {
                BTreeMap<Integer, Object> base = BTreeMap.empty();
                TreeMap<Integer, Object> baseRef = new TreeMap<>();
                for (int i = 0; i < size; i++)
                {
                    int key = random.nextInt(size * 4 + 1);
                    if (baseRef.containsKey(key))
                        continue;
                    Object value = new Object();
                    base = base.with(key, value);
                    baseRef.put(key, value);
                }

                BTreeMap<Integer, Object> edited = base;
                TreeMap<Integer, Object> editedRef = new TreeMap<>(baseRef);
                int edits = random.nextInt(Math.max(2, size / 4 + 2));
                for (int e = 0; e < edits; e++)
                {
                    int key = random.nextInt(size * 4 + 1);
                    switch (random.nextInt(4))
                    {
                        case 0: // put a new value, or a new key
                            Object value = new Object();
                            edited = edited.withForce(key, value);
                            editedRef.put(key, value);
                            break;
                        case 1: // replace with the same value instance: not a change
                            if (editedRef.containsKey(key))
                                edited = edited.withForce(key, editedRef.get(key));
                            break;
                        default: // remove
                            if (editedRef.containsKey(key))
                            {
                                edited = edited.without(key);
                                editedRef.remove(key);
                            }
                    }
                }

                assertEquals("size " + size + " round " + round, expected(baseRef, editedRef), actual(base, edited));
                assertEquals("reversed, size " + size + " round " + round, expected(editedRef, baseRef), actual(edited, base));
            }
        }
    }

    @Test
    public void mapsWithNoSharedStructureAreMerged()
    {
        Object shared = new Object();
        BTreeMap<Integer, Object> left = BTreeMap.empty();
        BTreeMap<Integer, Object> right = BTreeMap.empty();
        TreeMap<Integer, Object> leftRef = new TreeMap<>();
        TreeMap<Integer, Object> rightRef = new TreeMap<>();
        for (int i = 0; i < 2000; i++)
        {
            Object value = i % 3 == 0 ? new Object() : shared;
            if (i % 5 != 0)
            {
                left = left.with(i, value);
                leftRef.put(i, value);
            }
            if (i % 7 != 0)
            {
                right = right.with(i, value);
                rightRef.put(i, value);
            }
        }
        assertEquals(expected(leftRef, rightRef), actual(left, right));
    }

    @Test
    public void oneInsertIntoALargeMapVisitsOnlyItsPath()
    {
        int[] compares = new int[1];
        Comparator<Integer> counting = (a, b) -> {
            compares[0]++;
            return Integer.compare(a, b);
        };
        BTreeMap<Integer, Object> base = BTreeMap.empty(counting);
        for (int i = 0; i < 100_000; i += 2)
            base = base.with(i, new Object());

        for (int key : new int[]{ -1, 1, 50_001, 99_999, 100_001 })
        {
            BTreeMap<Integer, Object> edited = base.with(key, new Object());
            compares[0] = 0;
            List<String> diff = actual(base, edited);
            assertEquals(List.of("+" + key), diff);
            // A full walk would compare about 50k entries; the path is a few hundred at most.
            assertTrue("compares for one insert at " + key + ": " + compares[0], compares[0] < 1000);
        }

        BTreeMap<Integer, Object> removed = base.without(50_000);
        compares[0] = 0;
        assertEquals(List.of("-50000"), actual(base, removed));
        assertTrue("compares for one removal: " + compares[0], compares[0] < 1000);
    }

    private static List<String> expected(TreeMap<Integer, Object> left, TreeMap<Integer, Object> right)
    {
        TreeSet<Integer> keys = new TreeSet<>(left.keySet());
        keys.addAll(right.keySet());
        List<String> out = new ArrayList<>();
        for (int key : keys)
        {
            Object l = left.get(key);
            Object r = right.get(key);
            if (r == null)
                out.add("-" + key);
            else if (l == null)
                out.add("+" + key);
            else if (l != r)
                out.add("~" + key);
        }
        return out;
    }

    private static List<String> actual(BTreeMap<Integer, Object> left, BTreeMap<Integer, Object> right)
    {
        List<String> out = new ArrayList<>();
        BTreeMap.diff(left, right, new BTreeMap.DiffListener<>()
        {
            public void onlyLeft(Integer key, Object value)
            {
                out.add("-" + key);
            }

            public void onlyRight(Integer key, Object value)
            {
                out.add("+" + key);
            }

            public void changed(Integer key, Object l, Object r)
            {
                out.add("~" + key);
            }
        });
        return out;
    }
}
