/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.metrics;

/**
 * Exact cumulative scrape history using the smallest signed primitive width.
 * Null represents an all-zero snapshot. Callers serialize access and compute
 * the delta before packing the next snapshot, which may reuse the old array.
 * Inspired by OpenTelemetry's adaptive-width counters; no recording state is changed.
 */
final class AdaptiveHistogramHistory
{
    private AdaptiveHistogramHistory()
    {
    }

    static long[] delta(long[] now, Object last)
    {
        long[] result = now.clone();
        if (last instanceof byte[])
        {
            byte[] previous = (byte[]) last;
            for (int i = 0, size = Math.min(now.length, previous.length); i < size; i++)
                result[i] -= previous[i];
        }
        else if (last instanceof short[])
        {
            short[] previous = (short[]) last;
            for (int i = 0, size = Math.min(now.length, previous.length); i < size; i++)
                result[i] -= previous[i];
        }
        else if (last instanceof int[])
        {
            int[] previous = (int[]) last;
            for (int i = 0, size = Math.min(now.length, previous.length); i < size; i++)
                result[i] -= previous[i];
        }
        else if (last instanceof long[])
        {
            long[] previous = (long[]) last;
            for (int i = 0, size = Math.min(now.length, previous.length); i < size; i++)
                result[i] -= previous[i];
        }
        return result;
    }

    /** Takes ownership of now when long storage is needed; never retains the returned delta. */
    static Object pack(long[] now, Object previous)
    {
        long min = 0;
        long max = 0;
        for (long value : now)
        {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        if (min == 0 && max == 0)
            return null;
        if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE)
        {
            byte[] packed = previous instanceof byte[] && ((byte[]) previous).length == now.length
                            ? (byte[]) previous : new byte[now.length];
            for (int i = 0; i < now.length; i++)
                packed[i] = (byte) now[i];
            return packed;
        }
        if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE)
        {
            short[] packed = previous instanceof short[] && ((short[]) previous).length == now.length
                             ? (short[]) previous : new short[now.length];
            for (int i = 0; i < now.length; i++)
                packed[i] = (short) now[i];
            return packed;
        }
        if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE)
        {
            int[] packed = previous instanceof int[] && ((int[]) previous).length == now.length
                           ? (int[]) previous : new int[now.length];
            for (int i = 0; i < now.length; i++)
                packed[i] = (int) now[i];
            return packed;
        }
        return now;
    }
}
