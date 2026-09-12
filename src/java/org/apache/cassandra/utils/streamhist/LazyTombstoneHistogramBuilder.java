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
package org.apache.cassandra.utils.streamhist;

/**
 * Defers the reference builder's spool until the first observation. The empty delegate preserves
 * snapshot capacity and release behavior; all observations use the original spool and merge order.
 */
public final class LazyTombstoneHistogramBuilder implements TombstoneHistogramBuilder
{
    private final int maxBinSize;
    private final int roundSeconds;
    private int pendingSpoolSize;
    private StreamingTombstoneHistogramBuilder delegate;

    public LazyTombstoneHistogramBuilder(int maxBinSize, int maxSpoolSize, int roundSeconds)
    {
        assert maxBinSize > 0 && maxSpoolSize >= 0 && roundSeconds > 0: "Invalid arguments: maxBinSize:" + maxBinSize + " maxSpoolSize:" + maxSpoolSize + " delta:" + roundSeconds;

        this.maxBinSize = maxBinSize;
        this.roundSeconds = roundSeconds;
        // Preserve constructor failures and overflow behavior for unrepresentable spool sizes.
        boolean deferSpool = maxSpoolSize > 0 && maxSpoolSize <= (1 << 29);
        this.pendingSpoolSize = deferSpool ? maxSpoolSize : 0;
        this.delegate = new StreamingTombstoneHistogramBuilder(maxBinSize, deferSpool ? 0 : maxSpoolSize, roundSeconds);
    }

    public void update(long point)
    {
        update(point, 1);
    }

    public void update(long point, int value)
    {
        if (pendingSpoolSize > 0)
        {
            delegate = new StreamingTombstoneHistogramBuilder(maxBinSize, pendingSpoolSize, roundSeconds);
            pendingSpoolSize = 0;
        }
        delegate.update(point, value);
    }

    public void flushHistogram()
    {
        delegate.flushHistogram();
    }

    public TombstoneHistogram build()
    {
        return delegate.build();
    }

    public void releaseBuffers()
    {
        delegate.releaseBuffers();
        pendingSpoolSize = 0;
    }
}
