/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class FileUtilsTest
{
    private final String dummyFileName = "itIsSafeToDeleteThisFile.txt";

    @Test
    public void testCreateDirectory()
    {
        // On Windows, we have to break it down to individual steps as we cannot use File.mkdir() to
        // construct nested directories.
        FileUtils.createDirectory("test/nested/directory/creation");
    }

    @Test (expected = RuntimeException.class)
    public void testCopyNoDirectory() throws IOException
    {
        File f = new File(dummyFileName);
        if (f.exists())
            f.delete();
        FileUtils.copyFile(dummyFileName, "dummyDestDir", "dummyDestName", false);
    }

    @Test (expected = RuntimeException.class)
    public void testCopyOverwriteFail() throws IOException
    {
        populateDummyFile();

        // Attempt to copy a file over our existing w/out flag
        FileUtils.copyFile(dummyFileName, "", dummyFileName, false);
    }

    @Test
    public void testCopyAndOverwrite() throws IOException
    {
        populateDummyFile();
        Path newFile = Paths.get("test/new/file/" + dummyFileName);
        if (Files.exists(newFile))
            Files.delete(newFile);
        FileUtils.copyFile(dummyFileName, "test/new/file/", dummyFileName, false);
        FileUtils.copyFile(dummyFileName, "test/new/file/", dummyFileName, true);
    }

    private void populateDummyFile() throws IOException
    {
        if (Files.notExists(Paths.get(dummyFileName)))
        {
            Random r = new Random();
            byte dataToWrite[] = new byte[128];
            r.nextBytes(dataToWrite);
            FileOutputStream out = new FileOutputStream(dummyFileName);
            out.write(dataToWrite);
            out.close();
        }
    }
}
