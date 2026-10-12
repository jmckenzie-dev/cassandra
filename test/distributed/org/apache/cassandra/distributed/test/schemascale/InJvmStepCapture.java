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
import java.time.LocalDateTime;
import java.util.function.Consumer;

/**
 * Captures from the harness JVM, which also runs the node. The harness has no node-only flight recording,
 * so node.jfr is skipped. Heap and class numbers include the harness.
 */
public final class InJvmStepCapture extends JcmdStepCapture
{
    public InJvmStepCapture(Path javaHome, Consumer<String> warnings)
    {
        super(javaHome, () -> ProcessHandle.current().pid(), warnings, LocalDateTime::now, COMMAND_TIMEOUT, DUMP_TIMEOUT);
    }

    @Override
    boolean recordsFlights()
    {
        return false;
    }

    @Override
    String flightNote()
    {
        return "in-JVM mode uses the harness JVM: node.jfr is skipped and heap figures include the harness";
    }
}
