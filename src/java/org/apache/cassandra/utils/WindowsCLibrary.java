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

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

public final class WindowsCLibrary implements NativeFunctions
{
    private final int ENOMEM = 12;

    private final Logger logger = LoggerFactory.getLogger(WindowsCLibrary.class);

    boolean jnaAvailable = true;
    boolean jnaLockable = false;

    public WindowsCLibrary()
    {
        try
        {
            Native.register("ntdll");
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
            jnaAvailable = false;
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.warn("JNA link failure, one or more native method will be unavailable.");
            logger.warn("JNA link failure details: {}", e.getMessage());
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
            jnaAvailable = false;
        }
    }

    private static native int mlockall(int flags) throws LastErrorException;
    private static native int munlockall() throws LastErrorException;
    private static native int fcntl(int fd, int command, long flags) throws LastErrorException;
    private static native int posix_fadvise(int fd, long offset, int len, int flag) throws LastErrorException;
    private static native int open(String path, int flags) throws LastErrorException;
    private static native int fsync(int fd) throws LastErrorException;
    private static native int close(int fd) throws LastErrorException;

    private int errno(RuntimeException e)
    {
        assert e instanceof LastErrorException;
        try
        {
            return ((LastErrorException) e).getErrorCode();
        }
        catch (NoSuchMethodError x)
        {
            logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
            return 0;
        }
    }

    public boolean jnaAvailable()
    {
        return jnaAvailable;
    }

    public boolean jnaMemoryLockable()
    {
        return jnaLockable;
    }

    public void tryMlockall()
    {
        try
        {
            logger.warn("TODO: Handle Windows tryMlockall in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            // this will have already been logged by CLibrary, no need to repeat it
            logger.warn("EXPLORE: failed tryMlockall");
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            if (errno(e) == ENOMEM && System.getProperty("os.name").toLowerCase().contains("linux"))
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                        + " Increase RLIMIT_MEMLOCK or run Cassandra as root.");
            }
            else if (!System.getProperty("os.name").toLowerCase().contains("mac"))
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error {}", errno(e));
            }
        }
    }

    public void trySkipCache(String path, long offset, long len)
    {
        trySkipCache(getfd(path), offset, len);
    }

    public void trySkipCache(int fd, long offset, long len)
    {
        if (len == 0)
            trySkipCache(fd, 0, 0);

        while (len > 0)
        {
            int sublen = (int) Math.min(Integer.MAX_VALUE, len);
            trySkipCache(fd, offset, sublen);
            len -= sublen;
            offset -= sublen;
        }
    }

    public void trySkipCache(int fd, long offset, int len)
    {
        if (fd < 0) {
            logger.warn("EXPLORE: fd < 0 on trySkipCache");
            return;
        }

        try
        {
            logger.warn("TODO: Handle Windows trySkipCache in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.warn("EXPLORE: UnsatisfiedLinkError on trySkipCache");
            // if JNA is unavailable just skipping Direct I/O
            // instance of this class will act like normal RandomAccessFile
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("posix_fadvise(%d, %d) failed, errno (%d).", fd, offset, errno(e)));
        }
    }

    public int tryFcntl(int fd, int command, int flags)
    {
        // fcntl return value may or may not be useful, depending on the command
        int result = -1;

        try
        {
            logger.warn("TODO: Handle Windows tryFcntl in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
            logger.warn("EXPLORE: UnsatisfiedLinkError on tryFcntl");
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fcntl(%d, %d, %d) failed, errno (%d).", fd, command, flags, errno(e)));
        }

        return result;
    }

    public int tryOpenDirectory(String path)
    {
        int fd = -1;

        try
        {
            logger.warn("TODO: Handle Windows tryOpenDirectory in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
            logger.warn("EXPLORE: UnsatisfiedLinkError on tryOpenDirectory");
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("open(%s, O_RDONLY) failed, errno (%d).", path, errno(e)));
        }

        return fd;
    }

    public void trySync(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            logger.warn("TODO: Handle Windows fsync in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
            logger.warn("EXPLORE: UnsatisfiedLinkError on trySync");
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fsync(%d) failed, errno (%d).", fd, errno(e)));
        }
    }

    public void tryCloseFD(int fd)
    {
        if (fd == -1) {
            logger.warn("EXPLORE: fd -1 on tryCloseFD");
            return;
        }

        try
        {
            logger.warn("TODO: Handle Windows tryCloseFD in WindowsCLibrary");
        }
        catch (UnsatisfiedLinkError e)
        {
            // JNA is unavailable just skipping Direct I/O
            logger.warn("EXPLORE: UnsatisfiedLinkError on tryCloseFD");
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("close(%d) failed, errno (%d).", fd, errno(e)));
        }
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public int getfd(FileDescriptor descriptor)
    {
        Field field = FBUtilities.getProtectedField(descriptor.getClass(), "fd");

        if (field == null) {
            logger.warn("EXPLORE: field == null on getfd");
            return -1;
        }

        try
        {
            logger.warn("TODO: Handle Windows getfd in WindowsCLibrary");
        }
        catch (Exception e)
        {
            logger.warn("unable to read fd field from FileDescriptor");
        }

        return -1;
    }

    public int getfd(String path)
    {
        RandomAccessFile file = null;
        try
        {
            file = new RandomAccessFile(path, "r");
            return getfd(file.getFD());
        }
        catch (Throwable t)
        {
            // ignore
            logger.warn("EXPLORE: thrown on getfd");
            return -1;
        }
        finally
        {
            try
            {
                if (file != null)
                    file.close();
            }
            catch (Throwable t)
            {
                // ignore
            }
        }
    }
}
