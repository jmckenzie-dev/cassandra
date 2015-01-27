/*
 * 
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
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
public class CommitLogArchiver
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogArchiver.class);
    public static final SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
    private static final String DELIMITER = ",";
    static
    {
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    public final Map<String, Future<?>> archivePending = new ConcurrentHashMap<String, Future<?>>();
    private final ExecutorService executor = new JMXEnabledThreadPoolExecutor("CommitLogArchiver");
    final String archiveCommand;
    final String restoreCommand;
    final String restoreDirectories;
    public long restorePointInTime;
    public final TimeUnit precision;
    private ArrayList<NativeCommand> nativeCommands = new ArrayList<>();

    @VisibleForTesting
    public enum ArchiveCommandType
    {
        Copy,
        HardLink
    }

    private class NativeCommand
    {
        public ArchiveCommandType type;
        public String path;

        public NativeCommand(ArchiveCommandType type, String path)
        {
            this.type = type;
            this.path = path;
        }
    }

    public CommitLogArchiver()
    {
        Properties commitlog_commands = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("commitlog_archiving.properties"))
        {
            if (stream == null)
            {
                logger.debug("No commitlog_archiving properties found; archive + pitr will be disabled");
                archiveCommand = null;
                restoreCommand = null;
                restoreDirectories = null;
                restorePointInTime = Long.MAX_VALUE;
                precision = TimeUnit.MICROSECONDS;
            }
            else
            {
                commitlog_commands.load(stream);
                archiveCommand = commitlog_commands.getProperty("archive_command");
                restoreCommand = commitlog_commands.getProperty("restore_command");
                restoreDirectories = commitlog_commands.getProperty("restore_directories");
                if (restoreDirectories != null && !restoreDirectories.isEmpty())
                {
                    for (String dir : restoreDirectories.split(DELIMITER))
                    {
                        FileUtils.createDirectory(dir);
                    }
                }
                String targetTime = commitlog_commands.getProperty("restore_point_in_time");
                precision = TimeUnit.valueOf(commitlog_commands.getProperty("precision", "MICROSECONDS"));
                try
                {
                    restorePointInTime = Strings.isNullOrEmpty(targetTime) ? Long.MAX_VALUE : format.parse(targetTime).getTime();
                }
                catch (ParseException e)
                {
                    throw new RuntimeException("Unable to parse restore target time", e);
                }

                String copyCmd = commitlog_commands.getProperty("copy_on_archive");
                String hlCmd = commitlog_commands.getProperty("hard_link_on_archive");

                if (copyCmd != null && copyCmd.equals("true"))
                    nativeCommands.add(new NativeCommand(ArchiveCommandType.Copy, commitlog_commands.getProperty("copy_destination")));
                if (hlCmd != null && hlCmd.equals("true"))
                    nativeCommands.add(new NativeCommand(ArchiveCommandType.HardLink, commitlog_commands.getProperty("hard_link_destination")));

                // Confirm uniqueness of destination paths as we use original CommitLog name on file.
                for (int i = 0; i < nativeCommands.size(); ++i)
                    for (int j = i + 1; j < nativeCommands.size(); j++)
                        if (nativeCommands.get(i).path.equals(nativeCommands.get(j).path))
                            throw new RuntimeException("Overlapping paths on native commands in CommitLogArchiver.  Offenders: "
                                    + nativeCommands.get(i).type
                                    + " and "
                                    + nativeCommands.get(j).type);

                for (NativeCommand nc : nativeCommands)
                {
                    File directory = new File(nc.path);
                    FileUtils.createDirectory(directory);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to load commitlog_archiving.properties", e);
        }

    }

    public void maybeArchive(final CommitLogSegment segment)
    {
        for (final NativeCommand nc : nativeCommands)
        {
            final String destination = nc.path + File.separator + segment.getName();
            switch (nc.type)
            {
                case Copy:
                    archivePending.put(segment.getName(), executor.submit(new WrappedRunnable()
                    {
                        protected void runMayThrow() throws IOException
                        {
                            FileUtils.copyFile(segment.getPath(), nc.path, segment.getName(), false);
                        }
                    }));
                    break;
                case HardLink:
                    archivePending.put(segment.getName(), executor.submit(new WrappedRunnable()
                    {
                        protected void runMayThrow() throws IOException
                        {
                            FileUtils.createHardLink(segment.getPath(), destination);
                        }
                    }));
                    break;
            }
        }

        if (Strings.isNullOrEmpty(archiveCommand))
            return;

        archivePending.put(segment.getName(), executor.submit(new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                segment.waitForFinalSync();
                String command = archiveCommand.replace("%name", segment.getName());
                command = command.replace("%path", segment.getPath());
                exec(command);
            }
        }));
    }

    /**
     * Differs from the above because it can be used on any file, rather than only
     * managed commit log segments (and thus cannot call waitForFinalSync).
     *
     * Used to archive files present in the commit log directory at startup (CASSANDRA-6904)
     */
    public void maybeArchive(final String path, final String name)
    {
        if (Strings.isNullOrEmpty(archiveCommand))
            return;

        archivePending.put(name, executor.submit(new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException
            {
                String command = archiveCommand.replace("%name", name);
                command = command.replace("%path", path);
                exec(command);
            }
        }));
    }

    public boolean maybeWaitForArchiving(String name)
    {
        Future<?> f = archivePending.remove(name);
        if (f == null)
            return true; // archiving disabled

        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            if (e.getCause() instanceof IOException)
            {
                logger.error("Looks like the archiving of file {} failed earlier, cassandra is going to ignore this segment for now.", name);
                return false;
            }
            throw new RuntimeException(e);
        }

        return true;
    }

    public void maybeRestoreArchive()
    {
        if (Strings.isNullOrEmpty(restoreDirectories))
            return;

        for (String dir : restoreDirectories.split(DELIMITER))
        {
            File[] files = new File(dir).listFiles();
            if (files == null)
            {
                throw new RuntimeException("Unable to list directory " + dir);
            }
            for (File fromFile : files)
            {
                CommitLogDescriptor fromHeader = CommitLogDescriptor.fromHeader(fromFile);
                CommitLogDescriptor fromName = CommitLogDescriptor.isValid(fromFile.getName()) ? CommitLogDescriptor.fromFileName(fromFile.getName()) : null;
                CommitLogDescriptor descriptor;
                if (fromHeader == null && fromName == null)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, either from its name or its header: " + fromFile.getPath());
                else if (fromHeader != null && fromName != null && !fromHeader.equalsIgnoringCompression(fromName))
                    throw new IllegalStateException(String.format("Cannot safely construct descriptor for segment, as name and header descriptors do not match (%s vs %s): %s", fromHeader, fromName, fromFile.getPath()));
                else if (fromName != null && fromHeader == null && fromName.version >= CommitLogDescriptor.VERSION_21)
                    throw new IllegalStateException("Cannot safely construct descriptor for segment, as name descriptor implies a version that should contain a header descriptor, but that descriptor could not be read: " + fromFile.getPath());
                else if (fromHeader != null)
                    descriptor = fromHeader;
                else descriptor = fromName;

                if (descriptor.version > CommitLogDescriptor.current_version)
                    throw new IllegalStateException("Unsupported commit log version: " + descriptor.version);

                if (descriptor.compression != null) {
                    try
                    {
                        CompressionParameters.createCompressor(descriptor.compression);
                    }
                    catch (ConfigurationException e)
                    {
                        throw new IllegalStateException("Unknown compression", e);
                    }
                }

                File toFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
                if (toFile.exists())
                {
                    logger.debug("Skipping restore of archive {} as the segment already exists in the restore location {}",
                                 fromFile.getPath(), toFile.getPath());
                    continue;
                }

                String command = restoreCommand.replace("%from", fromFile.getPath());
                command = command.replace("%to", toFile.getPath());
                try
                {
                    exec(command);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void exec(String command) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command.split(" "));
        pb.redirectErrorStream(true);
        FBUtilities.exec(pb);
    }

    @VisibleForTesting
    public void addNativeCommand(ArchiveCommandType type, String path)
    {
        File f = new File(path);
        if (!f.exists())
            FileUtils.createDirectory(f);
        nativeCommands.add(new NativeCommand(type, path));
    }

    @VisibleForTesting
    public void clearNativeCommands()
    {
        nativeCommands.clear();
    }
}
