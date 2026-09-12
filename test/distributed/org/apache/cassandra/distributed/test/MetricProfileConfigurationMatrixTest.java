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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import static org.apache.cassandra.distributed.test.MetricProfileRegistrationTest.DEFAULT_CONFIGURATION;

public class MetricProfileConfigurationMatrixTest extends TestBaseImpl
{
    private final MetricProfileRegistrationTest fixture = new MetricProfileRegistrationTest();

    @Test
    public void legacyReservoirSupportsAllProfileWithAliases() throws Throwable
    {
        fixture.exerciseProfile("all_metrics.yml", true, DEFAULT_CONFIGURATION.withOptimizedMetrics(false));
    }

    @Test
    public void legacyReservoirSupportsSimpleProfileWithoutAliases() throws Throwable
    {
        fixture.exerciseProfile("simple_metrics.yml", false, DEFAULT_CONFIGURATION.withOptimizedMetrics(false));
    }

    @Test
    public void lazyMetricIdsSupportOriginalMetersAcrossTableLifecycle() throws Throwable
    {
        fixture.exerciseProfile("simple_metrics.yml", false, DEFAULT_CONFIGURATION.withLazyMetricIds(true));
    }

    @Test
    public void lazyMetricIdsSupportGeometricMetersAcrossTableLifecycle() throws Throwable
    {
        fixture.exerciseProfile("simple_metrics.yml", false,
                                DEFAULT_CONFIGURATION.withLazyMetricIds(true).withGeometricMeterArrays(true));
    }

    @Test
    public void adaptiveHistorySupportsStandardJmxRegistration() throws Throwable
    {
        fixture.exerciseProfile("simple_metrics.yml", false,
                                DEFAULT_CONFIGURATION.withCompactJmxRegistration(false).withAdaptiveJmxHistory(true));
    }
}
