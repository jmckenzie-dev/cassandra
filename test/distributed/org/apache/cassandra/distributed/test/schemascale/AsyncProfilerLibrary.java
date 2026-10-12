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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Extracts the native async-profiler library from lib/async-profiler-*.jar, for a node that loads it
 * with -agentpath. The jar holds macos/, linux-x64/ and linux-arm64/ copies of libasyncProfiler.so.
 */
final class AsyncProfilerLibrary
{
    static final String FILE_NAME = "libasyncProfiler.so";

    private AsyncProfilerLibrary() {}

    /** The jar entry for this platform, or null when the jar has none. */
    static String entryFor(String osName, String osArch)
    {
        String os = osName.toLowerCase(Locale.ROOT);
        String arch = osArch.toLowerCase(Locale.ROOT);
        if (os.contains("mac"))
            return "macos/" + FILE_NAME;
        if (os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64")))
            return "linux-x64/" + FILE_NAME;
        if (os.contains("linux") && (arch.equals("aarch64") || arch.equals("arm64")))
            return "linux-arm64/" + FILE_NAME;
        return null;
    }

    /** The newest lib/async-profiler-*.jar of the Cassandra home. */
    static Path findJar(Path cassandraHome) throws IOException
    {
        Path found = null;
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(cassandraHome.resolve("lib"), "async-profiler-*.jar"))
        {
            for (Path jar : jars)
                if (found == null || jar.getFileName().toString().compareTo(found.getFileName().toString()) > 0)
                    found = jar;
        }
        if (found == null)
            throw new IllegalStateException("No lib/async-profiler-*.jar under " + cassandraHome);
        return found;
    }

    /** Writes the library for this platform to {@code <directory>/libasyncProfiler.so} and returns the path. */
    static Path extract(Path jar, String entryName, Path directory) throws IOException
    {
        if (entryName == null)
            throw new IllegalStateException("async-profiler has no native library for this platform");
        Files.createDirectories(directory);
        Path target = directory.resolve(FILE_NAME).toAbsolutePath();
        try (ZipFile zip = new ZipFile(jar.toFile()))
        {
            ZipEntry entry = zip.getEntry(entryName);
            if (entry == null)
                throw new IllegalStateException(jar + " has no " + entryName);
            try (InputStream in = zip.getInputStream(entry))
            {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return target;
    }
}
