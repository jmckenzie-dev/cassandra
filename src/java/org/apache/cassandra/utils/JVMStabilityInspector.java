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
package org.apache.cassandra.utils;

import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.service.StorageService;

public class JVMStabilityInspector
{
    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    /**
     * Certain Throwables and Exceptions represent "Stop" conditions for the server.
     * @param t
     *      The Throwable to check for server-stop conditions
     */
    public static void inspectThrowable(Throwable t)
    {
        boolean isUnstable = false;
        if (t instanceof OutOfMemoryError)
            isUnstable = true;
        else if (t instanceof FileNotFoundException)
            isUnstable = true;
        if (isUnstable)
        {
            logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
            StorageService.instance.removeShutdownHook();
            System.exit(100);
        }
    }
}
