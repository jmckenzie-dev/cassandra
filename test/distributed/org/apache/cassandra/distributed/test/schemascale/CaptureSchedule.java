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

package org.apache.cassandra.distributed.test.schemascale;

/**
 * What the harness does at a table count when the profile sets captureAt: the step files, a saved schema,
 * a node profile and the restart probe. The probe waits for the node profile, so the profile sees the
 * DDL of a warm JVM; a saved schema restarts the node already, so that count has no extra probe.
 */
final class CaptureSchedule
{
    final boolean capture;
    final boolean snapshot;
    final boolean profile;
    final boolean restart;

    private CaptureSchedule(boolean capture, boolean snapshot, boolean profile, boolean restart)
    {
        this.capture = capture;
        this.snapshot = snapshot;
        this.profile = profile;
        this.restart = restart;
    }

    /**
     * @param last true at rampTables.max, where no DDL follows to profile
     * @param allowSnapshot false at the start count of a run from a saved schema
     * @param profiler true when the node has an async-profiler control
     */
    static CaptureSchedule at(ScaleProfile scale, long tables, boolean last, boolean allowSnapshot, boolean profiler)
    {
        boolean capture = tables <= Integer.MAX_VALUE && scale.captureAt.contains((int) tables);
        boolean snapshot = allowSnapshot && tables <= Integer.MAX_VALUE && scale.snapshotAt.contains((int) tables);
        boolean profile = capture && !last && profiler;
        boolean restart = capture && !snapshot && scale.captureRestart;
        return new CaptureSchedule(capture, snapshot, profile, restart);
    }

    boolean any()
    {
        return capture || snapshot;
    }
}
