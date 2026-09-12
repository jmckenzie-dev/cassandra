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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Metered;

import static org.apache.cassandra.config.CassandraRelevantProperties.GEOMETRIC_METER_ARRAYS;

/**
 * An interface which mimics {@link com.codahale.metrics.Meter} API and allows alternative implementations
 */
public interface Meter extends Metered
{
    /** Creates a meter that also extends {@link com.codahale.metrics.Meter}. */
    static Meter create()
    {
        boolean lazy = org.apache.cassandra.config.CassandraRelevantProperties.LAZY_METRIC_IDS.getBoolean();
        return GEOMETRIC_METER_ARRAYS.getBoolean()
               ? GeometricThreadLocalMeter.create(org.apache.cassandra.utils.MonotonicClock.Global.approxTime, lazy)
               : ThreadLocalMeter.create(org.apache.cassandra.utils.MonotonicClock.Global.approxTime, lazy);
    }

    void mark(long n);
    void mark();
}
