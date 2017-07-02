/*
 * Licensed to the Apache Software Foundation (ASF) under oneb
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
import java.util.*;
import java.util.stream.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sasi.*;
import org.apache.cassandra.index.sasi.disk.Descriptor.*;
import org.apache.cassandra.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.index.sasi.utils.*;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.index.sasi.disk.Descriptor.Version.*;
import static org.apache.cassandra.index.sasi.disk.TokenTreeBuilder.*;

// Note: all of the seek-able offsets contained in TokenTree should be sizeof(long)
// even if currently only lower int portion of them if used, because that makes
// it possible to switch to mmap implementation which supports long positions
// without any on-disk format changes and/or re-indexing if one day we'll have a need to.
public class TokenTree
{
    private final Descriptor descriptor;
    private final MappedBuffer file;
    private final long startPos;
    private final long treeMinToken;
    private final long treeMaxToken;
    private final long tokenCount;
    private final long blockCount;

    @VisibleForTesting
    protected TokenTree(MappedBuffer tokenTree)
    {
        this(Descriptor.CURRENT, tokenTree);
    }

    public TokenTree(Descriptor descriptor, MappedBuffer tokenTree)
    {
        this.descriptor = descriptor;
        file = tokenTree;
        startPos = file.position();
        file.position(startPos + TokenTreeBuilder.SHARED_HEADER_BYTES);

        validateMagic();

        tokenCount = file.getLong();
        blockCount = file.getShort();
        treeMinToken = file.getLong();
        treeMaxToken = file.getLong();
    }

    public long getCount()
    {
        return tokenCount;
    }

    public RangeIterator<Long, Token> iterator(KeyFetcher keyFetcher)
    {
        return new TokenTreeIterator(file.duplicate(), keyFetcher);
    }

    public OnDiskToken get(final long searchToken, KeyFetcher keyFetcher)
    {
        seekToLeaf(searchToken, file);
        long leafStart = file.position();
        short leafSize = file.getShort(leafStart + 1); // skip the info byte

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES); // skip to tokens
        short tokenIndex = searchLeaf(searchToken, leafSize);

        file.position(leafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);

        OnDiskToken token = getTokenAt(file, tokenIndex, leafSize, keyFetcher);

        return token.get().equals(searchToken) ? token : null;
    }

    private void validateMagic()
    {
        if (descriptor.version == aa)
            return;

        short magic = file.getShort();
        if (descriptor.version == Version.ab && magic == TokenTreeBuilder.AB_MAGIC)
            return;

        if (descriptor.version == Version.ac && magic == TokenTreeBuilder.AC_MAGIC)
            return;

        throw new IllegalArgumentException("invalid token tree. Written magic: '" + ByteBufferUtil.bytesToHex(file.getPageRegion(file.position() - 100, 200)) + "' " + ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(AC_MAGIC)));
    }

    // finds leaf that *could* contain token
    private void seekToLeaf(long token, MappedBuffer file)
    {
        // this loop always seeks forward except for the first iteration
        // where it may seek back to the root
        long blockStart = startPos;
        while (true)
        {
            file.position(blockStart);

            byte info = file.get();
            boolean isLeaf = (info & 1) == 1;

            if (isLeaf)
            {
                file.position(blockStart);
                break;
            }

            short tokenCount = file.getShort();

            long minToken = file.getLong();
            long maxToken = file.getLong();

            long seekBase = blockStart + BLOCK_HEADER_BYTES;
            if (minToken > token)
            {
                // seek to beginning of child offsets to locate first child
                file.position(seekBase + tokenCount * TOKEN_BYTES);
            }
            else if (maxToken < token)
            {
                // seek to end of child offsets to locate last child
                file.position(seekBase + (2 * tokenCount) * TOKEN_BYTES);
            }
            else
            {
                // skip to end of block header/start of interior block tokens
                file.position(seekBase);

                short offsetIndex = searchBlock(token, tokenCount, file);

                // file pointer is now at beginning of offsets
                if (offsetIndex == tokenCount)
                    file.position(file.position() + (offsetIndex * TOKEN_BYTES));
                else
                    file.position(file.position() + ((tokenCount - offsetIndex - 1) + offsetIndex) * TOKEN_BYTES);
            }
            blockStart = (startPos + (int) file.getLong());
        }
    }

    private short searchBlock(long searchToken, short tokenCount, MappedBuffer file)
    {
        short offsetIndex = 0;
        for (int i = 0; i < tokenCount; i++)
        {
            if (searchToken < file.getLong())
                break;

            offsetIndex++;
        }

        return offsetIndex;
    }

    private short searchLeaf(long searchToken, short tokenCount)
    {
        long base = file.position();

        int start = 0;
        int end = tokenCount;
        int middle = 0;

        while (start <= end)
        {
            middle = start + ((end - start) >> 1);
            long token = file.getLong(base + middle * LEAF_ENTRY_BYTES + (descriptor.version.compareTo(Version.ac) < 0 ? LEGACY_TOKEN_OFFSET_BYTES : 0));
            if (token == searchToken)
                break;

            if (token < searchToken)
                start = middle + 1;
            else
                end = middle - 1;
        }

        return (short) middle;
    }

    private class TokenTreeIterator extends RangeIterator<Long, Token>
    {
        private final KeyFetcher keyFetcher;
        private final MappedBuffer file;

        private long currentLeafStart;
        private int currentTokenIndex;

        private long leafMinToken;
        private long leafMaxToken;
        private short leafSize;

        protected boolean firstIteration = true;
        private boolean lastLeaf;

        TokenTreeIterator(MappedBuffer file, KeyFetcher keyFetcher)
        {
            super(treeMinToken, treeMaxToken, tokenCount);

            this.file = file;
            this.keyFetcher = keyFetcher;
        }

        protected Token computeNext()
        {
            maybeFirstIteration();

            if (currentTokenIndex >= leafSize && lastLeaf)
                return endOfData();

            if (currentTokenIndex < leafSize) // tokens remaining in this leaf
            {
                return getTokenAt(currentTokenIndex++);
            }
            else // no more tokens remaining in this leaf
            {
                assert !lastLeaf;

                seekToNextLeaf();
                setupBlock();
                return computeNext();
            }
        }

        protected void performSkipTo(Long nextToken)
        {
            maybeFirstIteration();

            if (nextToken <= leafMaxToken) // next is in this leaf block
            {
                searchLeaf(nextToken);
            }
            else // next is in a leaf block that needs to be found
            {
                seekToLeaf(nextToken, file);
                setupBlock();
                findNearest(nextToken);
            }
        }

        private void setupBlock()
        {
            currentLeafStart = file.position();
            currentTokenIndex = 0;

            lastLeaf = (file.get() & (1 << TokenTreeBuilder.LAST_LEAF_SHIFT)) > 0;
            leafSize = file.getShort();

            leafMinToken = file.getLong();
            leafMaxToken = file.getLong();

            // seek to end of leaf header/start of data
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_HEADER_BYTES);
        }

        private void findNearest(Long next)
        {
            if (next > leafMaxToken && !lastLeaf)
            {
                seekToNextLeaf();
                setupBlock();
                findNearest(next);
            }
            else if (next > leafMinToken)
                searchLeaf(next);
        }

        private void searchLeaf(long next)
        {
            for (int i = currentTokenIndex; i < leafSize; i++)
            {
                if (compareTokenAt(currentTokenIndex, next) >= 0)
                    break;

                currentTokenIndex++;
            }
        }

        private int compareTokenAt(int idx, long toToken)
        {
            return Long.compare(file.getLong(getTokenPosition(idx)), toToken);
        }

        private Token getTokenAt(int idx)
        {
            return TokenTree.this.getTokenAt(file, idx, leafSize, keyFetcher);
        }

        private long getTokenPosition(int idx)
        {
            // skip entry header to get position pointing directly at the entry's token
            return TokenTree.this.getEntryPosition(idx, file, descriptor) + (descriptor.version.compareTo(Version.ac) < 0 ? LEGACY_TOKEN_OFFSET_BYTES : 0);
        }

        private void seekToNextLeaf()
        {
            file.position(currentLeafStart + TokenTreeBuilder.BLOCK_BYTES);
        }

        public void close() throws IOException
        {
            // nothing to do here
        }

        private void maybeFirstIteration()
        {
            // seek to the first token only when requested for the first time,
            // highly predictable branch and saves us a lot by not traversing the tree
            // on creation time because it's not at all required.
            if (!firstIteration)
                return;

            seekToLeaf(treeMinToken, file);
            setupBlock();
            firstIteration = false;
        }
    }

    //
    public class OnDiskToken extends Token
    {
        private final Set<TokenInfo> info = new HashSet<>(2);
        private final Set<RowKey> loadedKeys = new TreeSet<>(RowKey.COMPARATOR);

        private OnDiskToken(MappedBuffer buffer, long position, short leafSize, KeyFetcher keyFetcher)
        {
            super(buffer.getLong(position + (descriptor.version.compareTo(Version.ac) < 0 ? LEGACY_TOKEN_OFFSET_BYTES : 0)));
            info.add(new TokenInfo(buffer, position, leafSize, keyFetcher, descriptor));
        }

        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            if (token != o.token)
                throw new IllegalArgumentException(String.format("%s != %s", token, o.token));

            if (o instanceof OnDiskToken)
            {
                info.addAll(((OnDiskToken) other).info);
            }
            else
            {
                Iterators.addAll(loadedKeys, o.iterator());
            }
        }

        public Iterator<RowKey> iterator()
        {
            List<Iterator<RowKey>> keys = new ArrayList<>(info.size());

            for (TokenInfo i : info)
            {
                keys.add(i.iterator());
            }

            if (!loadedKeys.isEmpty())
                keys.add(loadedKeys.iterator());

            return MergeIterator.get(keys, RowKey.COMPARATOR, new MergeIterator.Reducer<RowKey, RowKey>()
            {
                private RowKey reduced;

                public boolean trivialReduceIsTrivial()
                {
                    return true;
                }

                public void reduce(int idx, RowKey current)
                {
                    reduced = current;
                }

                protected RowKey getReduced()
                {
                    return reduced;
                }
            });
        }

        public KeyOffsets getOffsets()
        {
            KeyOffsets offsets = new KeyOffsets();
            for (TokenInfo i : info)
                offsets.merge(i.fetchOffsets());

            return offsets;
        }
    }

    private OnDiskToken getTokenAt(MappedBuffer buffer, int idx, short leafSize, KeyFetcher keyFetcher)
    {
        return new OnDiskToken(buffer, getEntryPosition(idx, buffer, descriptor), leafSize, keyFetcher);
    }

    private long getEntryPosition(int idx, MappedBuffer file, Descriptor descriptor)
    {
        if (descriptor.version.compareTo(Version.ac) < 0)
            return file.position() + (idx * LEGACY_LEAF_ENTRY_BYTES);

        // skip n entries, to the entry with the given index
        return file.position() + (idx * LEAF_ENTRY_BYTES);
    }

    private class TokenInfo
    {
        private final MappedBuffer buffer;
        private final KeyFetcher keyFetcher;
        private final Descriptor descriptor;
        private final long position;
        private final short leafSize;

        public TokenInfo(MappedBuffer buffer, long position, short leafSize, KeyFetcher keyFetcher, Descriptor descriptor)
        {
            this.keyFetcher = keyFetcher;
            this.buffer = buffer;
            this.position = position;
            this.leafSize = leafSize;
            this.descriptor = descriptor;
        }

        public Iterator<RowKey> iterator()
        {
            return fetchOffsets().getKeyIterator(keyFetcher);
        }

        public int hashCode()
        {
            return new HashCodeBuilder().append(keyFetcher).append(position).append(leafSize).build();
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof TokenInfo))
                return false;

            TokenInfo o = (TokenInfo) other;
            return keyFetcher == o.keyFetcher && position == o.position;

        }

        /**
         * Legacy leaf storage format (used for reading data formats before AC):
         *
         *    [(short) leaf type][(short) offset extra bytes][(long) token][(int) offsetData]
         *
         * Many pairs can be encoded into long+int.
         *
         * Simple entry: offset fits into (int)
         *
         *    [(short) leaf type][(short) offset extra bytes][(long) token][(int) offsetData]
         *
         * FactoredOffset: a single offset, offset fits into (long)+(int) bits:
         *
         *    [(short) leaf type][(short) 16 bytes of remained offset][(long) token][(int) top 32 bits of offset]
         *
         * PackedCollisionEntry: packs the two offset entries into int and a short (if both of them fit into
         * (long) and one of them fits into (int))
         *
         *    [(short) leaf type][(short) 16 the offset that'd fit into short][(long) token][(int) 32 bits of offset that'd fit into int]
         *
         * Otherwise, the rest gets packed into limited-size overflow collision entry
         *
         *    [(short) leaf type][(short) count][(long) token][(int) start index]
         */
        private KeyOffsets fetchOffsetsLegacy()
        {
            short info = buffer.getShort(position);
            // offset extra is unsigned short (right-most 16 bits of 48 bits allowed for an offset)
            int offsetExtra = buffer.getShort(position + Short.BYTES) & 0xFFFF;
            // is the it left-most (32-bit) base of the actual offset in the index file
            int offsetData = buffer.getInt(position + (2 * Short.BYTES) + Long.BYTES);

            EntryType type = EntryType.of(info & TokenTreeBuilder.ENTRY_TYPE_MASK);

            KeyOffsets rowOffsets = new KeyOffsets();
            switch (type)
            {
                case SIMPLE:
                    rowOffsets.put(offsetData, KeyOffsets.NO_OFFSET);
                    break;
                case OVERFLOW:
                    long offsetPos = (buffer.position() + (2 * (leafSize * Long.BYTES)) + (offsetData * Long.BYTES));

                    for (int i = 0; i < offsetExtra; i++)
                    {
                        long offset = buffer.getLong(offsetPos + (i * Long.BYTES));;
                        rowOffsets.put(offset, KeyOffsets.NO_OFFSET);
                    }
                    break;
                case FACTORED:
                    long offset = (((long) offsetData) << Short.SIZE) + offsetExtra;
                    rowOffsets.put(offset, KeyOffsets.NO_OFFSET);
                    break;
                case PACKED:
                    rowOffsets.put(offsetExtra, KeyOffsets.NO_OFFSET);
                    rowOffsets.put(offsetData, KeyOffsets.NO_OFFSET);
                default:
                    throw new IllegalStateException("Unknown entry type: " + type);
            }
            return rowOffsets;
        }

        /**
         * TODO: document storage format
         * @return
         */
        private KeyOffsets fetchOffsets()
        {
            // this os all correct b ut only for the dynamic leaf
            if (descriptor.version.compareTo(Version.ac) < 0)
                return fetchOffsetsLegacy();

            final long partitionDescriptionOffset;
            if (blockCount == 1)
            {
                final long tokenTreeOffset = startPos + BLOCK_HEADER_BYTES + tokenCount * LEAF_ENTRY_BYTES;
                partitionDescriptionOffset = tokenTreeOffset + buffer.getLong(position + TOKEN_BYTES);
            }
            else
            {
                final long tokenTreeOffset = startPos + blockCount * BLOCK_BYTES;
                partitionDescriptionOffset = tokenTreeOffset + buffer.getLong(position + TOKEN_BYTES);
            }

            KeyOffsets keyOffsets = new KeyOffsets();
            long readOffset = partitionDescriptionOffset;

            long partitionCount = buffer.get(readOffset); // position is relative
            readOffset += PARTITION_COUNT_BYTES;

            assert partitionCount > 0 : "Partition count: " + partitionCount + " " + ByteBufferUtil.bytesToHex(file.getPageRegion(readOffset, 30)) + " " + " " + tokenCount;

            for (long i = 0; i < partitionCount; i++)
            {
                long partitionPosition = buffer.getLong(readOffset);
                int rowCount = buffer.getInt(readOffset + PARTITION_OFFSET_BYTES);
                assert rowCount > 0;

                long baseRowOffset = readOffset + PARTITION_OFFSET_BYTES + ROW_COUNT_BYTES;

                for (int j = 0; j < rowCount; j++)
                {
                    long rowPosition = buffer.getLong(baseRowOffset);
                    keyOffsets.put(partitionPosition, rowPosition);
                    baseRowOffset += ROW_OFFSET_BYTES;
                }
                readOffset = baseRowOffset;
            }

            return keyOffsets;
        }
    }


}
