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

package org.apache.cassandra.db.commitlog;

import java.io.File;

import org.apache.cassandra.db.Mutation;

public interface CommitLogSegmentAllocator
{
    void start();
    void shutdown();

    /**
     * Indicates that a segment file has been flushed and is no longer needed. This generally performs blocking disk
     * operations so use with caution in critical path.
     *
     * @param segment segment to be discarded
     * @param delete  whether or not the segment is safe to be deleted.
     */
    void discard(CommitLogSegment segment, boolean delete);

    /**
     * Allocate a segment. This is always expected to succeed so should throw some form of exception on failure to
     * allocate; if you can't allocate a CLS, you can no longer write and the node is in a bad state.
     */
    CommitLogSegment.Allocation allocate(Mutation mutation, int size);

    /**
     * Hook to allow segment managers to track state surrounding creation of new segments. This takes place on a segment
     * management thread instead of the calling thread context.
     */
    CommitLogSegment createSegment();

    /**
     * When segments complete replay, the allocator has a hook to take action at that time.
     */
    void handleReplayedSegment(final File file);
}
