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
import java.nio.ByteBuffer;
import java.util.*;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractTokenTreeBuilder implements TokenTreeBuilder
{
    protected int numBlocks;

    protected Node root;
    protected InteriorNode rightmostParent;
    protected Leaf leftmostLeaf;
    protected Leaf rightmostLeaf;
    protected long tokenCount = 0;
    protected long treeMinToken;
    protected long treeMaxToken;

    public void add(TokenTreeBuilder other)
    {
        add(other.iterator());
    }

    public TokenTreeBuilder finish()
    {
        if (root == null)
            constructTree();

        return this;
    }

    public long getTokenCount()
    {
        return tokenCount;
    }

    public int serializedTokensSize()
    {
        if (numBlocks == 1)
            return BLOCK_HEADER_BYTES + ((int) tokenCount * LEAF_ENTRY_BYTES);
        else
            return numBlocks * BLOCK_BYTES;
    }

    public abstract void writePartitionDescription(DataOutputPlus out) throws IOException;

    protected void writeKeyOffsets(DataOutputPlus out, KeyOffsets offsets) throws IOException
    {
        assert offsets.size() < Byte.MAX_VALUE;
        out.writeByte(offsets.size());

        for (LongObjectCursor<long[]> cursor : offsets)
        {
            out.writeLong(cursor.key); // partition position (in index file)
            out.writeInt(cursor.value.length); // row count

            for (long rowPosition : cursor.value)
            {
                out.writeLong(rowPosition); // row position
            }
        }
    }

    // TODO: rename to `writeTree` or something
    public void writeTokens(DataOutputPlus out) throws IOException
    {
        ByteBuffer blockBuffer = ByteBuffer.allocate(serializedTokensSize());
        Iterator<Node> levelIterator = root.levelIterator();
        long childBlockIndex = 1;

        long offset = 0;
        while (levelIterator != null)
        {
            Node firstChild = null;
            while (levelIterator.hasNext())
            {
                Node block = levelIterator.next();

                if (firstChild == null && !block.isLeaf())
                    firstChild = ((InteriorNode) block).children.get(0);

                if (block.isSerializable())
                {
                    offset += block.serialize(childBlockIndex, blockBuffer, offset);
                    flushBuffer(blockBuffer, out, numBlocks != 1);
                }

                childBlockIndex += block.childCount();
            }

            levelIterator = (firstChild == null) ? null : firstChild.levelIterator();
        }
    }

    protected abstract void constructTree();

    protected void flushBuffer(ByteBuffer buffer, DataOutputPlus o, boolean align) throws IOException
    {
        // seek to end of last block before flushing
        if (align)
            alignBuffer(buffer, BLOCK_BYTES);

        buffer.flip();
        o.write(buffer);
        buffer.clear();
    }

    /**
     * Tree node,
     *
     * B+-tree consists of root, interior nodes and leaves. Root can be either a node or a leaf.
     *
     * Depending on the concrete implementation of {@code TokenTreeBuilder}
     * leaf can be partial or static (in case of {@code StaticTokenTreeBuilder} or dynamic in case
     * of {@code DynamicTokenTreeBuilder}
     */
    protected abstract class Node
    {
        protected InteriorNode parent;
        protected Node next;
        protected Long nodeMinToken, nodeMaxToken;

        public Node(Long minToken, Long maxToken)
        {
            nodeMinToken = minToken;
            nodeMaxToken = maxToken;
        }

        public abstract boolean isSerializable();
        public abstract long serialize(long childBlockIndex, ByteBuffer buf, long offset);
        public abstract int childCount();
        public abstract int tokenCount();

        public Long smallestToken()
        {
            return nodeMinToken;
        }

        public Long largestToken()
        {
            return nodeMaxToken;
        }

        public Iterator<Node> levelIterator()
        {
            return new LevelIterator(this);
        }

        public boolean isLeaf()
        {
            return (this instanceof Leaf);
        }

        protected boolean isLastLeaf()
        {
            return this == rightmostLeaf;
        }

        protected boolean isRoot()
        {
            return this == root;
        }

        protected void updateTokenRange(long token)
        {
            nodeMinToken = nodeMinToken == null ? token : Math.min(nodeMinToken, token);
            nodeMaxToken = nodeMaxToken == null ? token : Math.max(nodeMaxToken, token);
        }

        protected void serializeHeader(ByteBuffer buf)
        {
            Header header;
            if (isRoot())
                header = new RootHeader();
            else if (!isLeaf())
                header = new InteriorNodeHeader();
            else
                header = new LeafHeader();

            header.serialize(buf);
            alignBuffer(buf, BLOCK_HEADER_BYTES);
        }

        /**
         * Shared header part, written for all node types:
         *     [  info byte  ] [  token count   ] [ min node token ] [ max node token ]
         *     [      1b     ] [    2b (short)  ] [   8b (long)    ] [    8b (long)   ]
         **/
        private abstract class Header
        {
            /**
             * Serializes the shared part of the header
             */
            public void serialize(ByteBuffer buf)
            {
                buf.put(infoByte())
                   .putShort((short) (tokenCount()))
                   .putLong(nodeMinToken)
                   .putLong(nodeMaxToken);
            }

            protected abstract byte infoByte();
        }

        /**
         * In addition to shared header part, root header stores version information,
         * overall token count and min/max tokens for the whole tree:
         *     [      magic    ] [  overall token count  ] [ min tree token ] [ max tree token ]
         *     [   2b (short)  ] [         8b (long)     ] [   8b (long)    ] [    8b (long)   ]
         */
        private class RootHeader extends Header
        {
            public void serialize(ByteBuffer buf)
            {
                super.serialize(buf);
                writeMagic(buf);
//                if (tokenCount > 500)
//                    System.out.println("WRITING tokenCount = " + tokenCount);
//                System.out.println("numBlocks = " + numBlocks);
//                System.out.println("numBlocks = " + numBlocks);
                buf.putLong(tokenCount)
                   .putLong(numBlocks)
                   .putLong(treeMinToken)
                   .putLong(treeMaxToken);
            }

            protected byte infoByte()
            {
                // if leaf, set leaf indicator and last leaf indicator (bits 0 & 1)
                // if not leaf, clear both bits
                return isLeaf() ? ENTRY_TYPE_MASK : 0;
            }

            protected void writeMagic(ByteBuffer buf)
            {
                switch (Descriptor.CURRENT_VERSION)
                {
                    case ab:
                        buf.putShort(AB_MAGIC);
                        break;
                    case ac:
                        buf.putShort(AC_MAGIC);
                        break;
                    default:
                        throw new RuntimeException("Unsupported version");
                }

            }

            // TODO: to string
        }

        private class InteriorNodeHeader extends Header
        {
            // bit 0 (leaf indicator) & bit 1 (last leaf indicator) cleared
            protected byte infoByte()
            {
                return 0;
            }
        }

        private class LeafHeader extends Header
        {
            // bit 0 set as leaf indicator
            // bit 1 set if this is last leaf of data
            protected byte infoByte()
            {
                byte infoByte = 1;
                infoByte |= (isLastLeaf()) ? (1 << LAST_LEAF_SHIFT) : 0;

                return infoByte;
            }
        }

    }

    /**
     * TODO: fix format
     * Leaf consists of
     *   - header (format described in {@code Header} )
     *   - data (format described in {@code LeafEntry})
     *   - overflow collision entries, that hold {@value OVERFLOW_TRAILER_CAPACITY} of {@code RowOffset}.
     */
    protected abstract class Leaf extends Node
    {
        public Leaf(Long minToken, Long maxToken)
        {
            super(minToken, maxToken);
        }

        public int childCount()
        {
            return 0;
        }

        public long serialize(long childBlockIdx, ByteBuffer buf, long offset)
        {
            serializeHeader(buf);
            return serializeData(buf, offset);
        }

        protected abstract long serializeData(ByteBuffer buf, long offset);

        protected long serializeToken(ByteBuffer buf, Long token, KeyOffsets offsets, final long offset)
        {
            long tokenOffset = PARTITION_COUNT_BYTES;  // partition count
            for (LongObjectCursor<long[]> cursor : offsets)
            {
                // TODO: solve through multiplication
                // OR EVEN BETTER: denormalize it / make it a helper method on @KeyOffsets
                tokenOffset += PARTITION_OFFSET_BYTES + ROW_COUNT_BYTES; // partition offset, row count
                tokenOffset += cursor.value.length * ROW_OFFSET_BYTES; // row positions
            }

            buf.putLong(token);
            buf.putLong(offset); // TODO: make it int?

            return tokenOffset;
        }
    }

    /**
     * Interior node consists of:
     *    - (interior node) header
     *    - tokens (serialized as longs, with count stored in header)
     *    - child offsets
     */
    protected class InteriorNode extends Node
    {
        protected List<Long> tokens = new ArrayList<>(TOKENS_PER_BLOCK);
        protected List<Node> children = new ArrayList<>(TOKENS_PER_BLOCK + 1);
        protected int position = 0;

        public InteriorNode()
        {
            super(null, null);
        }

        public boolean isSerializable()
        {
            return true;
        }

        public long serialize(long childBlockIndex, ByteBuffer buf, long offset)
        {
            serializeHeader(buf);
            serializeTokens(buf);
            serializeChildOffsets(childBlockIndex, buf);
            // interior node doesn't change the offset of the partition description
            return 0;
        }

        public int childCount()
        {
            return children.size();
        }

        public int tokenCount()
        {
            return tokens.size();
        }

        public Long smallestToken()
        {
            return tokens.get(0);
        }

        protected void add(Long token, InteriorNode leftChild, InteriorNode rightChild)
        {
            int pos = tokens.size();
            if (pos == TOKENS_PER_BLOCK)
            {
                InteriorNode sibling = split();
                sibling.add(token, leftChild, rightChild);

            }
            else
            {
                if (leftChild != null)
                    children.add(pos, leftChild);

                if (rightChild != null)
                {
                    children.add(pos + 1, rightChild);
                    rightChild.parent = this;
                }

                updateTokenRange(token);
                tokens.add(pos, token);
            }
        }

        protected void add(Leaf node)
        {
            if (position == (TOKENS_PER_BLOCK + 1))
            {
                rightmostParent = split();
                rightmostParent.add(node);
            }
            else
            {

                node.parent = this;
                children.add(position, node);
                position++;

                // the first child is referenced only during bulk load. we don't take a value
                // to store into the tree, one is subtracted since position has already been incremented
                // for the next node to be added
                if (position - 1 == 0)
                    return;


                // tokens are inserted one behind the current position, but 2 is subtracted because
                // position has already been incremented for the next add
                Long smallestToken = node.smallestToken();
                updateTokenRange(smallestToken);
                tokens.add(position - 2, smallestToken);
            }

        }

        protected InteriorNode split()
        {
            Pair<Long, InteriorNode> splitResult = splitBlock();
            Long middleValue = splitResult.left;
            InteriorNode sibling = splitResult.right;
            InteriorNode leftChild = null;

            // create a new root if necessary
            if (parent == null)
            {
                parent = new InteriorNode();
                root = parent;
                sibling.parent = parent;
                leftChild = this;
                numBlocks++;
            }

            parent.add(middleValue, leftChild, sibling);

            return sibling;
        }

        protected Pair<Long, InteriorNode> splitBlock()
        {
            final int splitPosition = TOKENS_PER_BLOCK - 2;
            InteriorNode sibling = new InteriorNode();
            sibling.parent = parent;
            next = sibling;

            Long middleValue = tokens.get(splitPosition);

            for (int i = splitPosition; i < TOKENS_PER_BLOCK; i++)
            {
                if (i != TOKENS_PER_BLOCK && i != splitPosition)
                {
                    long token = tokens.get(i);
                    sibling.updateTokenRange(token);
                    sibling.tokens.add(token);
                }

                Node child = children.get(i + 1);
                child.parent = sibling;
                sibling.children.add(child);
                sibling.position++;
            }

            for (int i = TOKENS_PER_BLOCK; i >= splitPosition; i--)
            {
                if (i != TOKENS_PER_BLOCK)
                    tokens.remove(i);

                if (i != splitPosition)
                    children.remove(i);
            }

            nodeMinToken = smallestToken();
            nodeMaxToken = tokens.get(tokens.size() - 1);
            numBlocks++;

            return Pair.create(middleValue, sibling);
        }

        protected boolean isFull()
        {
            return (position >= TOKENS_PER_BLOCK + 1);
        }

        private void serializeTokens(ByteBuffer buf)
        {
            tokens.forEach(buf::putLong);
        }

        private void serializeChildOffsets(long childBlockIndex, ByteBuffer buf)
        {
            for (int i = 0; i < children.size(); i++)
                buf.putLong((childBlockIndex + i) * BLOCK_BYTES);
        }
    }

    public static class LevelIterator extends AbstractIterator<Node>
    {
        private Node currentNode;

        LevelIterator(Node first)
        {
            currentNode = first;
        }

        public Node computeNext()
        {
            if (currentNode == null)
                return endOfData();

            Node returnNode = currentNode;
            currentNode = returnNode.next;

            return returnNode;
        }
    }


    protected static void alignBuffer(ByteBuffer buffer, int blockSize)
    {
        long curPos = buffer.position();
        if ((curPos & (blockSize - 1)) != 0) // align on the block boundary if needed
            buffer.position((int) FBUtilities.align(curPos, blockSize));
    }

}
