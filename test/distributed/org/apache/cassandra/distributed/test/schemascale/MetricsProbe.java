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

import java.util.Map;

/**
 * Reads node-side numbers. The keys are the same in every implementation; a value of -1 means
 * the number is not available.
 * <ul>
 * <li>JVM: heap_used, heap_max, heap_post_gc, direct_max, metaspace_used, gc_count, gc_time_ms, threads,
 * alloc_bytes_total</li>
 * <li>Node: mbeans, scheduled_tasks, node_keyspaces, node_tables</li>
 * <li>TCM: tcm_epoch, tcm_commit_p99_ns, snapshot_bytes_computed, snapshot_bytes_gauge,
 * snapshot_store_failures, max_mutation_size, snapshot_size_limit (the smaller of max_value_size and
 * internode_max_message_size: the snapshot size the node can store and send)</li>
 * <li>Storage: system_schema_bytes, metadata_log_bytes</li>
 * </ul>
 */
public interface MetricsProbe
{
    /**
     * @param withSnapshotSize when true, also compute snapshot_bytes_computed. This costs O(schema).
     */
    Map<String, Long> sample(boolean withSnapshotSize);

    default Map<String, Long> sample()
    {
        return sample(false);
    }

    /** Asks the node JVM for a full garbage collection. */
    void gc();

    /** Bytes in system_schema, on disk plus in memtables. Cheaper than {@link #sample}. */
    default long systemSchemaBytes()
    {
        return sample(false).getOrDefault("system_schema_bytes", -1L);
    }
}
