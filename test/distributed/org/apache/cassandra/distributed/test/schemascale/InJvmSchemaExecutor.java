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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

/** Sends CQL through the coordinator of node 1. */
public final class InJvmSchemaExecutor implements SchemaExecutor
{
    private final Cluster cluster;

    public InJvmSchemaExecutor(Cluster cluster)
    {
        this.cluster = cluster;
    }

    @Override
    public void execute(String cql)
    {
        cluster.coordinator(1).execute(cql, ConsistencyLevel.ALL);
    }

    @Override
    public Object[][] query(String cql, Object... binds)
    {
        return cluster.coordinator(1).execute(cql, ConsistencyLevel.ONE, binds);
    }
}
