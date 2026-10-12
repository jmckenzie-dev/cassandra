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

/** Builds a node runtime. The harness calls it once for the main node and once per grid point. */
public interface RuntimeFactory
{
    /**
     * @param directory working directory for this node
     * @param nodeConfig cassandra.yaml overrides, in YAML value types
     */
    NodeRuntime create(Path directory, Map<String, Object> nodeConfig) throws Exception;

    /**
     * Asks the next {@link #create} to use these ports, so a node started from saved data keeps the addresses
     * that the cluster metadata holds. Keys: storagePort, sslStoragePort, nativeTransportPort, jmxPort. A
     * factory without fixed ports ignores it.
     */
    default void preferPorts(Map<String, ?> ports)
    {
    }
}
