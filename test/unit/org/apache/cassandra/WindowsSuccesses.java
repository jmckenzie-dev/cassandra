/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import org.apache.cassandra.io.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class WindowsSuccesses
{
    private static StandardOpenOption[] openOptions = new StandardOpenOption[3];
    private static File fromFile = new File("contended.txt");
    private static File renameFile = new File("renamed.txt");
    private static Path fromPath = Paths.get("contended.txt");
    private static Path renamePath = Paths.get("renamed.txt");

    {
        openOptions[0] = StandardOpenOption.CREATE;
        openOptions[1] = StandardOpenOption.READ;
        openOptions[2] = StandardOpenOption.WRITE;
    }

    @Before
    public void cleanup() throws IOException, InterruptedException
    {
        System.gc();
        Thread.sleep(100);
        if (Files.exists(fromPath))
            Files.delete(fromPath);
        if (Files.exists(renamePath))
            Files.delete(renamePath);
    }

    @Test
    public void testDeleteChannelFail() throws IOException
    {
        FileChannel channel = FileChannel.open(fromPath, openOptions);
        FileChannel channel_2 = FileChannel.open(fromPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.write(createRandomBB(1024));
        Files.delete(fromPath);
        assert !Files.exists(fromPath);
        channel.close();
        channel_2.close();
    }

    @Test
    public void testRenameChannelFail() throws IOException
    {
        FileChannel channel = FileChannel.open(fromPath, openOptions);
        channel.write(createRandomBB(1024));
        FileUtils.renameWithConfirm(fromFile, renameFile);
        assert !Files.exists(fromPath);
    }

    private ByteBuffer createRandomBB(int size)
    {
        ByteBuffer toWrite = ByteBuffer.allocate(1024);
        byte[] buffer = new byte[1024];
        new Random().nextBytes(buffer);
        toWrite.put(buffer);
        return toWrite;
    }
}
