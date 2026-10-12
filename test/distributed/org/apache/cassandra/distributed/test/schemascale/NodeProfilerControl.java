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
 * The node's AsyncProfilerService MBean ({@code org.apache.cassandra.profiler:type=AsyncProfiler}) and how
 * the node loads the native library. The node must start with -Dcassandra.async_profiler.enabled=true;
 * {@link #execute} also needs -Dcassandra.async_profiler.unsafe_mode=true.
 */
public interface NodeProfilerControl
{
    /** AsyncProfilerMBean.isEnabled: the service has a loaded profiler. */
    boolean enabled() throws Exception;

    /** AsyncProfilerMBean.execute: any async-profiler command. Throws a SecurityException cause without unsafe mode. */
    String execute(String command) throws Exception;

    /** AsyncProfilerMBean.start with the events, outputFormat, duration and outputFileName keys. */
    boolean start(Map<String, String> parameters) throws Exception;

    /** AsyncProfilerMBean.stop. */
    boolean stop(Map<String, String> parameters) throws Exception;

    /** AsyncProfilerMBean.fetch of a file that {@link #start} wrote. */
    byte[] fetch(String resultFile) throws Exception;

    /** Where {@link #start} writes its files on this host ({@code <log dir>/profiler}), or null when unknown. */
    Path resultDirectory();

    /** How the running node loaded the library: "mbean" (AsyncProfiler.getInstance) or "agentpath". */
    String loadPath();

    /** Makes the next node start load the library with -agentpath. Returns false when not possible. */
    boolean useAgentOnNextStart();
}
