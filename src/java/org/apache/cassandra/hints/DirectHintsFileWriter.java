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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

import static org.apache.cassandra.utils.FBUtilities.updateChecksum;

class DirectHintsFileWriter implements HintsFileWriter
{
    private FileChannel channel;
    private CRC32 crc;

    public DirectHintsFileWriter(FileChannel channel, CRC32 crc)
    {
        this.channel = channel;
        this.crc = crc;
    }

    public void write(ByteBuffer bufferedHints, ByteBuffer singleHint) throws IOException
    {
        write(bufferedHints);
        write(singleHint);
    }

    public void write(ByteBuffer buffer) throws IOException
    {
        // update file-global CRC checksum
        updateChecksum(crc, buffer);
        channel.write(buffer);
    }
}
