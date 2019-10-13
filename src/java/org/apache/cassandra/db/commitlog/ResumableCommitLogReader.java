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

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * A CommitLogReader that allows both a) indicating how much of the CommitLog the user would like to read, and b) the ability
 * to resume reading from a given offset to prevent the need to re-read CommitLog files in a CDC / consumption context.
 * As this is purely a class for use in CDC-enabled contexts, we don't test its operation in non-cdc and assert that CDC
 * is enabled if this class is in use.
 *
 * Note: As the plan for usage of this class is by external users writing CDC consumers, treat the public members of this
 * class as an external API contract. i.e. don't make changes to it lightly, and consider backwards-compatibility options
 * on changes. In the event that this interface proves to be more fluid over time than the previous stability of the CommitLog
 * in general would imply, we can break out a proper interface for it at that time.
 */
public class ResumableCommitLogReader
{
    public ResumableCommitLogReader()
    {
        if (!DatabaseDescriptor.isCDCEnabled())
        {
            throw new RuntimeException("Cannot use a ResumableCommitLogReader if CDC is not enabled.");
        }
    }
}
