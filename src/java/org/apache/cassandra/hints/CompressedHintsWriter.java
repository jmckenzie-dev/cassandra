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

package org.apache.cassandra.hints;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.SyncUtil;

final class CompressedHintsWriter extends HintsWriter
{
    private static final int COMPRESSED_HEADER_SIZE = 4 + 4; // uncompressed len + checksum
    private ByteBuffer compressedBuffer;
    private volatile long lastWrittenPos = 0;
    private CRC32 writeCRC = new CRC32();

    CompressedHintsWriter(File directory, HintsDescriptor descriptor) throws IOException
    {
        super(directory, descriptor);
        compressedBuffer = ByteBuffer.allocate(0);
        lastWrittenPos = channel.position();
    }

    HintsDescriptor descriptor()
    {
        return descriptor;
    }

    @Override
    protected void write(ByteBuffer bufferedHints) throws IOException
    {
        write(bufferedHints, null);
    }

    @Override
    protected void write(ByteBuffer buffer, ByteBuffer optionalBuffer) throws IOException
    {
        try
        {
            int bufferedHintLength = buffer.remaining();
            if (optionalBuffer != null)
                bufferedHintLength += optionalBuffer.remaining();
            int neededBufferSize = hintsCompressor.initialCompressedBufferLength(bufferedHintLength + COMPRESSED_HEADER_SIZE);

            // Lazy init compressedBuffer to desired size
            if (hintsCompressor.preferredBufferType() != BufferType.typeOf(compressedBuffer) ||
                compressedBuffer.capacity() < neededBufferSize)
            {
                FileUtils.clean(compressedBuffer);
                compressedBuffer = hintsCompressor.preferredBufferType().allocate(neededBufferSize);
            }

            /* Visualization of format:
                  [(int)Len] [(int)Checksum] [CompressedData]
            */
            compressedBuffer.putInt(bufferedHintLength);

            compressedBuffer.limit(compressedBuffer.capacity()).position(COMPRESSED_HEADER_SIZE);
            hintsCompressor.compress(buffer.duplicate(), compressedBuffer);
            if (optionalBuffer != null)
                hintsCompressor.compress(optionalBuffer.duplicate(), compressedBuffer);

            compressedBuffer.flip().position(COMPRESSED_HEADER_SIZE);
            writeCRC.reset();
            writeCRC.update(compressedBuffer);

            compressedBuffer.putInt(4, (int)writeCRC.getValue());
            compressedBuffer.rewind();

            // TODO: Add to metric tracking total used compressed disk space
            channel.write(compressedBuffer);
            long val = channel.position();
            assert channel.position() - lastWrittenPos == compressedBuffer.limit();
            lastWrittenPos = channel.position();
            SyncUtil.force(channel, true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, file);
        }
        channel.write(buffer);
    }
}
