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

import java.nio.file.Path;
import java.util.Map;

/**
 * One Cassandra node under test. The in-JVM implementation wraps a dtest cluster. The process
 * implementation runs bin/cassandra as its own JVM. The workload only sees this interface.
 */
public interface NodeRuntime extends AutoCloseable
{
    /** Starts the node for the first time and blocks until it serves requests. */
    void start() throws Exception;

    /** Drains and stops the node. */
    void stopGraceful() throws Exception;

    /** Stops the node without a drain, so the next start replays the commit log. */
    void kill() throws Exception;

    /** Starts a node that {@link #stopGraceful} or {@link #kill} stopped. Blocks until it serves requests. */
    void restart() throws Exception;

    SchemaExecutor executor();

    MetricsProbe probe();

    /** Operating system process id of the node JVM. */
    long pid();

    /** The node system.log, or null when it is unknown. The file may not exist. */
    Path logFile();

    /** Facts about the node for summary.json, such as ports, limits and JVM arguments. May be empty. */
    default Map<String, Object> summary()
    {
        return Map.of();
    }

    /** The node's async-profiler service, or null when this node has none. */
    default NodeProfilerControl profilerControl()
    {
        return null;
    }

    /** Stops the node if it still runs and frees every resource. Never throws. */
    @Override
    void close();
}
