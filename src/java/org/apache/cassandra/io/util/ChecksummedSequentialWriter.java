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
package org.apache.cassandra.io.util;

import java.io.File;

import org.apache.cassandra.io.sstable.Descriptor;

import static org.apache.cassandra.utils.Throwables.maybeFail;

public class ChecksummedSequentialWriter extends SequentialWriter
{
    private final SequentialWriter crcWriter;
    private final DataIntegrityMetadata.ChecksumWriter crcMetadata;

    public ChecksummedSequentialWriter(File file, int bufferSize, File crcPath)
    {
        super(file, bufferSize);
        crcWriter = new SequentialWriter(crcPath, 8 * 1024);
        crcMetadata = new DataIntegrityMetadata.ChecksumWriter(crcWriter.stream);
        crcMetadata.writeChunkSize(buffer.length);
    }

    protected void flushData()
    {
        super.flushData();
        crcMetadata.append(buffer, 0, validBufferBytes, false);
    }

    protected void doPrepare(Descriptor descriptor)
    {
        super.doPrepare(descriptor);
        if (descriptor != null)
            crcMetadata.writeFullChecksum(descriptor);
        crcWriter.prepareToCommit(descriptor);
    }

    public Throwable commit(Throwable accumulate)
    {
        return crcWriter.commit(super.commit(accumulate));
    }

    public Throwable abort(Throwable accumulate)
    {
        return crcWriter.abort(super.abort(accumulate));
    }

    public Throwable cleanup(Throwable accumulate)
    {
        return crcWriter.cleanup(super.cleanup(accumulate));
    }
}
