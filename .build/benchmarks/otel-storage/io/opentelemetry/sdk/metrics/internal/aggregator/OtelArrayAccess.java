/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.opentelemetry.sdk.metrics.internal.aggregator;

/** Test access to the actual package-private OTel implementation, without extra retained wrappers. */
public final class OtelArrayAccess
{
    private OtelArrayAccess() {}

    public static Object create(int length) { return new AdaptingIntegerArray(length); }
    public static void add(Object array, int index, long delta) { ((AdaptingIntegerArray) array).increment(index, delta); }
    public static long get(Object array, int index) { return ((AdaptingIntegerArray) array).get(index); }
    public static void clear(Object array) { ((AdaptingIntegerArray) array).clear(); }
    public static Object copy(Object array) { return ((AdaptingIntegerArray) array).copy(); }
    public static int length(Object array) { return ((AdaptingIntegerArray) array).length(); }
}
