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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;

public final class CLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(CLibrary.class);

    private static final NativeFunctions nativeFunctions;

    static
    {
        if (FBUtilities.isUnix())
            nativeFunctions = new LinuxCLibrary();
        else
            nativeFunctions = new WindowsCLibrary();
    }

    private CLibrary() {}

    public static boolean jnaAvailable()
    {
        return nativeFunctions.jnaAvailable();
    }

    public static void tryMlockall()
    {
        nativeFunctions.tryMlockall();
    }

    public static void trySkipCache(String path, long offset, long len)
    {
        nativeFunctions.trySkipCache(path, offset, len);
    }

    public static void trySkipCache(int fd, long offset, long len)
    {
        nativeFunctions.trySkipCache(fd, offset, len);
    }

    public static void trySkipCache(int fd, long offset, int len)
    {
        nativeFunctions.trySkipCache(fd, offset, len);
    }

    public static int tryFcntl(int fd, int command, int flags)
    {
        return nativeFunctions.tryFcntl(fd, command, flags);
    }

    public static int tryOpenDirectory(String path)
    {
        return nativeFunctions.tryOpenDirectory(path);
    }

    public static void trySync(int fd)
    {
        nativeFunctions.trySync(fd);
    }

    public static void tryCloseFD(int fd)
    {
        nativeFunctions.tryCloseFD(fd);
    }

    public static int getfd(FileDescriptor descriptor)
    {
        return nativeFunctions.getfd(descriptor);
    }

    public static int getfd(String path)
    {
        return nativeFunctions.getfd(path);
    }
}
