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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Takes JVM diagnostics of the node. A capture never throws and never stops a ramp: a failed command
 * logs a warning and the other files still get written.
 */
public interface StepCapture
{
    /**
     * Writes the files of one step into the directory: class-histogram.txt, heap-info.txt, nmt-summary.txt,
     * threads.txt and, when the node records flight data, node.jfr with the time since the previous step.
     */
    void captureStep(long tables, Path directory);

    /**
     * Writes the stop artifacts into the directory: class-histogram.txt, nmt-diff.txt, thread-dump.txt,
     * node.jfr and, when {@code heapDump} is true, heap.hprof. Run it before the node stops.
     *
     * @param heapUsedHint used heap bytes of the node, or -1 when unknown; the heap dump needs twice that much free disk
     * @return facts for summary.json
     */
    Map<String, Object> captureFinal(Path directory, long heapUsedHint, boolean heapDump);

    /**
     * Live heap bytes of the node from a class histogram, or -1 when not available. The histogram collects
     * first, so it confirms a heap stop: a post-GC reading under a concurrent collector can include GBs of
     * floating garbage (13.0 to 21.0 GB in one step of the stock run, with a linear live total).
     *
     * @param scratch file for the histogram output; overwritten
     */
    default long liveHeapBytes(Path scratch)
    {
        return -1;
    }

    /** The bytes on the "Total" line of a jcmd GC.class_histogram file, or -1. */
    static long histogramTotal(Path histogram)
    {
        try
        {
            for (String line : Files.readAllLines(histogram, StandardCharsets.UTF_8))
            {
                String[] fields = line.trim().split("\\s+");
                if (fields.length == 3 && fields[0].equals("Total"))
                    return Long.parseLong(fields[2]);
            }
        }
        catch (IOException | NumberFormatException e)
        {
            return -1;
        }
        return -1;
    }
}
