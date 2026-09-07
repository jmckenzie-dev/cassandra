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
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class TombstonePageContinuationTest
{
    @Test
    public void rejectsStaticAndEmptyClusterings()
    {
        for (Clustering<?> clustering : new Clustering<?>[]{ Clustering.STATIC_CLUSTERING, Clustering.EMPTY })
            assertThatThrownBy(() -> new TombstonePageContinuation(clustering)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void serializationPreservesCompositeClustering()
    {
        qt().withFixedSeed(18424).withExamples(100).forAll(integers().all()).checkAssert(seed -> {
            Random random = new Random(seed);
            ByteBuffer[] values = new ByteBuffer[1 + random.nextInt(8)];
            for (int i = 0; i < values.length; i++)
            {
                switch (random.nextInt(3))
                {
                    case 0:
                        values[i] = null;
                        break;
                    case 1:
                        values[i] = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                        break;
                    default:
                        byte[] bytes = new byte[1 + random.nextInt(128)];
                        random.nextBytes(bytes);
                        values[i] = ByteBuffer.wrap(bytes);
                }
            }
            TombstonePageContinuation continuation = new TombstonePageContinuation(Clustering.make(values));
            for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.current_version })
            {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    TombstonePageContinuation.serializer.serialize(continuation, out, version);
                    assertEquals(out.position(), TombstonePageContinuation.serializer.serializedSize(continuation, version));
                    try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                    {
                        assertEquals(continuation, TombstonePageContinuation.serializer.deserialize(in, version));
                        assertEquals(0, in.available());
                    }
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            }
        });
    }
}
