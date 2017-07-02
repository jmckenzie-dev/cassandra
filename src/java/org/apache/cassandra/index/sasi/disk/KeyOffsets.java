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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.annotations.VisibleForTesting;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.KeyFetcher;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
// TODO: ASSERT SORTED INVARIANT!!!!

// TODO: We do not need a hashmap here, we can optimise it for the default case (no collisions)
// and handle collisions with an array and hardcore copying since they will never happen
public class KeyOffsets
{
    /// ??? DENORMALIZE SIZE CALCULATIONS
    private final LongObjectOpenHashMap<LongArrayList> container;

    public static final long NO_OFFSET = Long.MIN_VALUE;

    public KeyOffsets() {
        this(new LongObjectOpenHashMap<>(2));
    }

    private KeyOffsets(LongObjectOpenHashMap container) {
        this.container = container;
    }

    public void merge(KeyOffsets other)
    {
        for (LongObjectCursor<LongArrayList> cursor : other.container)
            put(cursor.key, cursor.value);
    }

    public KeyOffsets copy()
    {
        KeyOffsets copy = new KeyOffsets();
        for (LongObjectCursor<LongArrayList> outer : container)
            copy.container.put(outer.key, outer.value.clone());
        return copy;
    }

    public void put(long currentPartitionOffset, long currentRowOffset)
    {
        LongArrayList arr = container.get(currentPartitionOffset);
        if (arr == null)
            container.put(currentPartitionOffset, asArray(currentRowOffset));
        else
            arr.add(currentRowOffset);
    }

    public void put(long currentPartitionOffset, LongArrayList currentRowOffset)
    {
        LongArrayList arr = container.get(currentPartitionOffset);

        if (arr == null)
            arr = container.put(currentPartitionOffset, currentRowOffset);
        else
            arr.addAll(currentRowOffset);
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof KeyOffsets))
            return false;

        KeyOffsets other = (KeyOffsets) obj;
        if (other.container.size() != this.container.size())
            return false;

        if (!other.container.equals(this.container))
            return false;

        return true;
    }

    public byte partitionCount()
    {
        return (byte) container.size();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("KeyOffsets { ");
        container.forEach((a, b) -> {
            LongArrayList list = new LongArrayList();
            sb.append(a).append(": ").append(Arrays.toString(list.toArray()));
        });
        sb.append(" }");
        return sb.toString();
    }

    // primitive array creation
    public static LongArrayList asArray(long... val)
    {
        LongArrayList res = new LongArrayList();
        for (long l : val)
            res.add(l);
        return res;
    }

    public KeyIterator getKeyIterator(KeyFetcher fetcher)
    {
        return new KeyIterator(fetcher);
    }

    public void iteratate(KeyOffsetIterator iterator) throws IOException
    {
        for (LongObjectCursor<LongArrayList> cursor: container)
        {
            iterator.onPartition(cursor.key, cursor.value.size());

            for (LongCursor rowPosition : cursor.value)
                iterator.onRow(rowPosition.value);
        }
    }

    public Iterable<PartitionCursor> iteratate()
    {
        return () ->
               new Iterator<PartitionCursor>()
               {
                   PartitionCursor partitionCursor = new PartitionCursor();
                   Iterator<LongObjectCursor<LongArrayList>> outerIterator = container.iterator();

                   public boolean hasNext()
                   {
                       return outerIterator.hasNext();
                   }

                   public PartitionCursor next()
                   {
                       LongObjectCursor<LongArrayList> cursor = outerIterator.next();
                       partitionCursor.reset(cursor.key, cursor.value.size());
                       return partitionCursor;
                   }
               };

    }

    public class PartitionCursor
    {
        public long partitionPosition;
        public int rowCount;

        public void reset(long partitionPosition, int rowCount)
        {
            this.partitionPosition = partitionPosition;
            this.rowCount = rowCount;
        }
    }

    public interface KeyOffsetIterator
    {
        public void onPartition(long partitionPosition, int rowCount) throws IOException;
        public void onRow(long rowPosition) throws IOException;
    }

    // TODO: make this iterator hierarchical!
    // This can be a "bottom part", filtering can be done furehter up as a transformation
    // Add `skipTo` support
    // Combine with the rest of the token tree, so that offsets could be fetched /pre-fetched on the go as well
    public class KeyIterator extends AbstractIterator<RowKey>
    {
        private final KeyFetcher keyFetcher;
        private final Iterator<LongObjectCursor<LongArrayList>> offsets;

        // This has to be completely factored out. We have to have a hierarchical iterator starting here.
        private DecoratedKey currentPartitionKey;
        private Iterator<LongCursor> currentCursor = null;

        private KeyIterator(KeyFetcher keyFetcher)
        {
            this.keyFetcher = keyFetcher;
            this.offsets = container.iterator();
        }

        public RowKey computeNext()
        {
            if (currentCursor != null && currentCursor.hasNext())
            {
                return keyFetcher.getRowKey(currentPartitionKey, currentCursor.next().value);
            }
            else if (offsets.hasNext())
            {
                LongObjectCursor<LongArrayList> cursor = offsets.next();
                currentPartitionKey = keyFetcher.getPartitionKey(cursor.key);
                currentCursor = cursor.value.iterator();

                if (currentCursor.hasNext())
                    return keyFetcher.getRowKey(currentPartitionKey, currentCursor.next().value);
            }

            return endOfData();
        }
    }
}
