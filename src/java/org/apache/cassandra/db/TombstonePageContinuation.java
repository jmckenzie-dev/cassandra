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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/** An exclusive resume position for a tombstone-limited single-partition response. */
public final class TombstonePageContinuation
{
    public static final IVersionedSerializer<TombstonePageContinuation> serializer = new Serializer();

    public final Clustering<?> clustering;

    public TombstonePageContinuation(Clustering<?> clustering)
    {
        Preconditions.checkArgument(clustering != Clustering.STATIC_CLUSTERING && !clustering.isEmpty());
        this.clustering = clustering;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof TombstonePageContinuation && clustering.equals(((TombstonePageContinuation) other).clustering);
    }

    @Override
    public int hashCode()
    {
        return clustering.hashCode();
    }

    private static class Serializer implements IVersionedSerializer<TombstonePageContinuation>
    {
        public void serialize(TombstonePageContinuation continuation, DataOutputPlus out, int version) throws IOException
        {
            Clustering<?> clustering = continuation.clustering;
            out.writeUnsignedVInt32(clustering.size());
            for (int i = 0; i < clustering.size(); i++)
                ByteBufferUtil.nullableByteBufferSerializer.serialize(clustering.bufferAt(i), out);
        }

        public TombstonePageContinuation deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = in.readUnsignedVInt32();
            if (count <= 0)
                throw new IOException("Invalid tombstone continuation component count: " + Integer.toUnsignedString(count));

            // Allocate for decoded components, not the untrusted count in the message header.
            List<ByteBuffer> values = new ArrayList<>();
            for (int i = 0; i < count; i++)
                values.add(ByteBufferUtil.nullableByteBufferSerializer.deserialize(in));
            return new TombstonePageContinuation(Clustering.make(values.toArray(new ByteBuffer[0])));
        }

        public long serializedSize(TombstonePageContinuation continuation, int version)
        {
            Clustering<?> clustering = continuation.clustering;
            long size = TypeSizes.sizeofUnsignedVInt(clustering.size());
            for (int i = 0; i < clustering.size(); i++)
                size += ByteBufferUtil.nullableByteBufferSerializer.serializedSize(clustering.bufferAt(i));
            return size;
        }
    }
}
