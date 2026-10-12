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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** The point where a ramp stopped, and why. */
public final class BreakPoint
{
    private static final int MAX_FRAMES = 10;

    public enum Reason
    {
        DDL_P99, HEAP_FRACTION, HEAP_BYTES, SNAPSHOT_SIZE, TIMEOUT, EXCEPTION
    }

    public final String ramp;
    public final long step;
    public final long tables;
    public final long columns;
    public final Reason reason;
    public final double value;
    public final double threshold;
    public final String exception;
    public final List<String> frames;

    private BreakPoint(String ramp, long step, long tables, long columns, Reason reason, double value,
                       double threshold, String exception, List<String> frames)
    {
        this.ramp = ramp;
        this.step = step;
        this.tables = tables;
        this.columns = columns;
        this.reason = reason;
        this.value = value;
        this.threshold = threshold;
        this.exception = exception;
        this.frames = frames;
    }

    public static BreakPoint threshold(String ramp, long step, ScaleState state, Reason reason, double value,
                                       double threshold)
    {
        return new BreakPoint(ramp, step, state.tables, state.columns, reason, value, threshold, null, List.of());
    }

    public static BreakPoint exception(String ramp, long step, ScaleState state, Throwable failure)
    {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root)
            root = root.getCause();
        List<String> frames = new ArrayList<>();
        for (StackTraceElement element : root.getStackTrace())
        {
            if (frames.size() == MAX_FRAMES)
                break;
            frames.add(element.toString());
        }
        return new BreakPoint(ramp, step, state.tables, state.columns, Reason.EXCEPTION, 0, 0,
                              failure.toString() + (root == failure ? "" : " / root cause: " + root), frames);
    }

    public Map<String, Object> toMap()
    {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ramp", ramp);
        values.put("step", step);
        values.put("tables", tables);
        values.put("columns", columns);
        values.put("reason", reason.name());
        values.put("value", value);
        values.put("threshold", threshold);
        values.put("exception", exception);
        values.put("frames", frames);
        return values;
    }

    @Override
    public String toString()
    {
        String text = String.format(Locale.ROOT, "%s at ramp %s step %d (%d tables, %d columns): value=%.3f threshold=%.3f",
                                    reason, ramp, step, tables, columns, value, threshold);
        if (exception != null)
            text += "\n    " + exception + "\n    at " + String.join("\n    at ", frames);
        return text;
    }
}
