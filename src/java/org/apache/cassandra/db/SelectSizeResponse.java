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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SelectSizeResponse
{
    public static final IVersionedSerializer<SelectSizeResponse> serializer = new Serializer();
    public final long partitionSize;

    public SelectSizeResponse(long partitionSize)
    {
        this.partitionSize = partitionSize;
    }

    private static class Serializer implements IVersionedSerializer<SelectSizeResponse>
    {
        public void serialize(SelectSizeResponse response, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(response.partitionSize);
        }

        public SelectSizeResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            return new SelectSizeResponse(in.readLong());
        }

        public long serializedSize(SelectSizeResponse response, int version)
        {
            return TypeSizes.sizeof(response.partitionSize);
        }
    }
}
