/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogArchiver.ArchiveCommandType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.utils.FBUtilities;

public class CommitLogArchiverTest extends SchemaLoader
{
    private static String KEYSPACE = "Keyspace1";
    private static String CF = "Standard1";

    @BeforeClass
    public static void setupSchema()
    {
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @Test
    public void testCopyCommand() throws IOException
    {
        testNativeArchiveCommand(ArchiveCommandType.Copy, "build/test/CLATestCopy/");
    }

    @Test
    public void testHardLinkCommand() throws IOException
    {
        testNativeArchiveCommand(ArchiveCommandType.HardLink, "build/test/CLATestHardLink/");
    }

    private void testNativeArchiveCommand(ArchiveCommandType type, String outputDir) throws IOException
    {
        File testFolder = new File(outputDir);
        if (testFolder.exists())
        {
            // Note: cannot use FileUtils.deleteRecursively since File.exists() returns false on symlinks and we want to delete them
            File[] contents = testFolder.listFiles();
            for (File toDelete : contents)
                toDelete.delete();
            testFolder.delete();
        }

        CommitLog.instance.archiver.clearNativeCommands();
        CommitLog.instance.archiver.addNativeCommand(type, outputDir);
        CommitLog.instance.resetUnsafe(false);

        Mutation rm = new RowUpdateBuilder(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF).metadata, 1, ByteBufferUtil.bytes("k"))
            .clustering("1")
            .add("val", ByteBuffer.allocate(DatabaseDescriptor.getCommitLogMaxCompressionBuffersInPool() / 4))
            .build();

        // Add a few segments
        CommitLog.instance.add(rm);
        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segments, got " + CommitLog.instance.activeSegments();

        // force archival to take place
        CommitLog.instance.forceRecycleAllSegments();

        File[] directoryListing = testFolder.listFiles();
        assert directoryListing.length == 1 : "Expected 1 file archived, got: " + directoryListing.length;
    }
}
