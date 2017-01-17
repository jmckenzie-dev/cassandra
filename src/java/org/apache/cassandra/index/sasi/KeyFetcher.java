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

package org.apache.cassandra.index.sasi;

import java.io.IOException;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.disk.*;
import org.apache.cassandra.io.*;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.utils.Pair;


public interface KeyFetcher
{
    public Clustering getClustering(long offset);
    public Unfiltered getUnfiltered(Clustering clustering, long offset);
    public Pair<DecoratedKey, Row> getPartitionHeader(long offset);

    public RowKey getRowKey(DecoratedKey key, Row staticRow, long rowOffset);

    /**
     * Fetches clustering and partition key from the sstable.
     *
     * Currently, clustering key is fetched from the data file of the sstable and partition key is
     * read from the index file. Reading from index file helps us to warm up key cache in this case.
     */
    public static class SSTableKeyFetcher implements KeyFetcher
    {
        private final SSTableReader sstable;

        public SSTableKeyFetcher(SSTableReader reader)
        {
            sstable = reader;
        }

        @Override
        public Clustering getClustering(long offset)
        {
            try
            {
                return sstable.clusteringAt(offset);
            }
            catch (IOException e)
            {
                throw new FSReadError(new IOException("Failed to read clustering from " + sstable.descriptor, e), sstable.getFilename());
            }
        }

        public Unfiltered getUnfiltered(Clustering clustering, long offset)
        {
            try
            {
                return sstable.unfilteredAt(clustering, offset);
            }
            catch (IOException e)
            {
                throw new FSReadError(new IOException("Failed to read clustering from " + sstable.descriptor, e), sstable.getFilename());
            }
        }

        public Pair<DecoratedKey, Row> getPartitionHeader(long offset)
        {
            try
            {
                return sstable.partitionHeaderAt(offset);
            }
            catch (IOException e)
            {
                throw new FSReadError(new IOException("Failed to read key from " + sstable.descriptor, e), sstable.getFilename());
            }
        }

        @Override
        public RowKey getRowKey(DecoratedKey key, Row staticRow, long rowOffset)
        {
            if (rowOffset == KeyOffsets.NO_OFFSET)
                // TODO: in this case we'll have to fall back to reading a whole partition?
                // This one is for compatibility reasons I assume?..
                return new RowKey(key, null, null, null, sstable.metadata().comparator);
            else
            {
                Clustering c = getClustering(rowOffset);
                return new RowKey(key, c, staticRow, (clustering) -> getUnfiltered(c, rowOffset), sstable.metadata().comparator);
            }
        }


        public int hashCode()
        {
            return sstable.descriptor.hashCode();
        }

        public boolean equals(Object other)
        {
            return other instanceof SSTableKeyFetcher
                   && sstable.descriptor.equals(((SSTableKeyFetcher) other).sstable.descriptor);
        }
    }

}
