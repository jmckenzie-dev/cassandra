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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reads restart timings from the part of system.log that a restart wrote. */
final class RestartLogParser
{
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS");
    private static final Pattern STAMP = Pattern.compile("(\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d,\\d{3})");
    private static final Pattern COMMITLOG = Pattern.compile("Log replay complete, \\d+ replayed mutations in (\\d+) ms");
    private static final String NO_COMMITLOG = "No commitlog files found";
    private static final String LOG_READY = "Marking LocalLog ready at epoch";

    final long logReplayMs;
    final long commitlogReplayMs;

    private RestartLogParser(long logReplayMs, long commitlogReplayMs)
    {
        this.logReplayMs = logReplayMs;
        this.commitlogReplayMs = commitlogReplayMs;
    }

    /**
     * log_replay_ms is the time from the first log line after the restart to the "LocalLog ready" line.
     * commitlog_replay_ms is the time CommitLog reports, or 0 when it found no commit log files. A value of -1 means the marker is missing.
     */
    static RestartLogParser parse(Path log, long offset)
    {
        if (log == null || !Files.isRegularFile(log))
            return new RestartLogParser(-1, -1);

        List<String> lines;
        try
        {
            byte[] all = Files.readAllBytes(log);
            String text = new String(all, (int) Math.min(offset, all.length), all.length - (int) Math.min(offset, all.length),
                                     StandardCharsets.UTF_8);
            lines = text.lines().toList();
        }
        catch (IOException e)
        {
            return new RestartLogParser(-1, -1);
        }

        long first = -1;
        long ready = -1;
        long commitlog = -1;
        for (String line : lines)
        {
            Matcher stamp = STAMP.matcher(line);
            if (stamp.find())
            {
                long millis = LocalDateTime.parse(stamp.group(1), TIMESTAMP).toInstant(ZoneOffset.UTC).toEpochMilli();
                if (first < 0)
                    first = millis;
                if (ready < 0 && line.contains(LOG_READY))
                    ready = millis;
            }
            if (line.contains(NO_COMMITLOG))
                commitlog = 0;
            Matcher replay = COMMITLOG.matcher(line);
            if (replay.find())
                commitlog = Long.parseLong(replay.group(1));
        }
        return new RestartLogParser(first >= 0 && ready >= 0 ? ready - first : -1, commitlog);
    }
}
