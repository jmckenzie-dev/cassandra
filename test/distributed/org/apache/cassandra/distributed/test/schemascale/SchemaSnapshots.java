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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.JsonUtils;

/**
 * Saved node data at a known schema size, so a run can start at N tables instead of creating them again.
 * A saved schema lives in {@code <root>/<schema key>/t<N>/}: the node {@code data/} directory (data,
 * commitlog, saved_caches, hints, cdc_raw; the cluster metadata log is in the system keyspaces) and a
 * {@code manifest.json}. The schema key hashes the table schema, the keyspace spread, the table naming and
 * the Cassandra version. The node config may differ between the run that saves and the run that restores;
 * the schema key must not.
 */
public final class SchemaSnapshots
{
    static final String MANIFEST = "manifest.json";
    static final String DATA = "data";
    // bump when the table or keyspace naming of ScaleWorkload changes
    static final String NAMING = "v1:ks_%03d.t%06d";
    private static final long COPY_TIMEOUT_HOURS = 6;

    /** Copies a directory tree to a target path that does not exist yet. Returns the copy method. */
    public interface Copier
    {
        String copy(Path source, Path target) throws IOException;
    }

    private final Path root;
    private final Copier copier;
    private final Supplier<Instant> clock;

    public SchemaSnapshots(Path root, Copier copier, Supplier<Instant> clock)
    {
        this.root = root.toAbsolutePath();
        this.copier = copier;
        this.clock = clock;
    }

    /** Uses an APFS clone (macOS) or a reflink copy (Linux) when it can, else a plain copy. */
    public static SchemaSnapshots create(Path root)
    {
        return new SchemaSnapshots(root, SchemaSnapshots::fastCopy, Instant::now);
    }

    public Path root()
    {
        return root;
    }

    /** Directory name such as ks1-3f2a9c01b7de. Same key, same table names and schema in system_schema. */
    public static String schemaKey(String tableSchema, int keyspaces, String cassandraVersion)
    {
        String text = NAMING + "|keyspaces=" + keyspaces + "|schema=" + tableSchema.trim().replaceAll("\\s+", " ") +
                      "|cassandra=" + cassandraVersion;
        try
        {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder("ks").append(keyspaces).append('-');
            for (int i = 0; i < 6; i++)
                hex.append(Character.forDigit((digest[i] >> 4) & 0xf, 16)).append(Character.forDigit(digest[i] & 0xf, 16));
            return hex.toString();
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public Path directory(String key, long tables)
    {
        return root.resolve(key).resolve("t" + tables);
    }

    /**
     * Copies the node data directory to {@code <root>/<key>/t<tables>/data} and writes manifest.json. Stop the
     * node first. The copy goes to a .partial directory and is renamed at the end, so a failed copy never
     * looks like a saved schema.
     *
     * @param facts extra manifest entries, such as keyspaces, git SHA and source run
     * @return the manifest, plus the directory
     */
    public Map<String, Object> save(Path nodeData, String key, long tables, Map<String, Object> facts) throws IOException
    {
        Path target = directory(key, tables);
        if (Files.exists(target))
            throw new IllegalStateException("A saved schema exists at " + target + "; remove it to save again");
        if (!Files.isDirectory(nodeData))
            throw new IllegalStateException("No node data directory at " + nodeData);
        Path partial = target.resolveSibling(target.getFileName() + ".partial");
        deleteTree(partial);
        Files.createDirectories(partial);
        long startedAt = System.nanoTime();
        String method = copier.copy(nodeData, partial.resolve(DATA));
        long copyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("schemaKey", key);
        manifest.put("tables", tables);
        manifest.putAll(facts);
        manifest.put("savedAt", clock.get().toString());
        manifest.put("copyMethod", method);
        manifest.put("copyMs", copyMs);
        manifest.put("bytes", treeBytes(partial.resolve(DATA)));
        Files.writeString(partial.resolve(MANIFEST), JsonUtils.writeAsPrettyJsonString(manifest), StandardCharsets.UTF_8);
        Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE);
        Map<String, Object> result = new LinkedHashMap<>(manifest);
        result.put("directory", target.toString());
        return result;
    }

    /** A snapshot directory: startFrom itself when it is a directory, else {@code <root>/<key>/<startFrom>}. */
    public Path resolve(String startFrom, String key)
    {
        Path direct = Path.of(startFrom);
        if (Files.isDirectory(direct))
            return direct.toAbsolutePath();
        Path named = root.resolve(key).resolve(startFrom);
        if (Files.isDirectory(named))
            return named;
        throw new IllegalArgumentException("startFrom '" + startFrom + "' is not a directory, and " + named + " does not exist");
    }

    public static Map<String, Object> readManifest(Path snapshot) throws IOException
    {
        Path file = snapshot.resolve(MANIFEST);
        if (!Files.isRegularFile(file))
            throw new IllegalStateException("No " + MANIFEST + " in " + snapshot);
        return new LinkedHashMap<>(JsonUtils.<Object>fromJsonMap(Files.readAllBytes(file)));
    }

    /**
     * Checks the schema key of the saved schema, then copies its data to nodeData. nodeData must not exist
     * or must be empty.
     *
     * @return the manifest, plus restoredFrom, restoreMethod and restoreMs
     * @throws IllegalStateException when the schema key differs
     */
    public Map<String, Object> restore(Path snapshot, String key, Path nodeData) throws IOException
    {
        Map<String, Object> manifest = readManifest(snapshot);
        Object saved = manifest.get("schemaKey");
        if (!key.equals(saved))
            throw new IllegalStateException("Refusing saved schema " + snapshot + ": its schema key is " + saved +
                                            " and this run needs " + key +
                                            ". The table schema, keyspace spread or Cassandra version differ.");
        if (!(manifest.get("tables") instanceof Number))
            throw new IllegalStateException(MANIFEST + " in " + snapshot + " has no table count");
        Path source = snapshot.resolve(DATA);
        if (!Files.isDirectory(source))
            throw new IllegalStateException("No " + DATA + " directory in " + snapshot);
        if (Files.isDirectory(nodeData))
        {
            try (Stream<Path> entries = Files.list(nodeData))
            {
                if (entries.findAny().isPresent())
                    throw new IllegalStateException("Node data directory " + nodeData + " is not empty");
            }
            Files.delete(nodeData);
        }
        Files.createDirectories(nodeData.toAbsolutePath().getParent());
        long startedAt = System.nanoTime();
        String method = copier.copy(source, nodeData);
        Map<String, Object> result = new LinkedHashMap<>(manifest);
        result.put("restoredFrom", snapshot.toString());
        result.put("restoreMethod", method);
        result.put("restoreMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt));
        return result;
    }

    /** cp -c (macOS clonefile) or cp --reflink=auto (Linux); a plain Java copy when cp fails or is not used. */
    static String fastCopy(Path source, Path target) throws IOException
    {
        String os = CassandraRelevantProperties.OS_NAME.getString();
        List<String> command = new ArrayList<>();
        String method;
        if (os.startsWith("Mac"))
        {
            command.addAll(List.of("cp", "-c", "-R"));
            method = "cp -c (APFS clone)";
        }
        else if (os.startsWith("Linux"))
        {
            command.addAll(List.of("cp", "-a", "--reflink=auto"));
            method = "cp -a --reflink=auto";
        }
        else
        {
            javaCopy(source, target);
            return "java";
        }
        command.add(source.toString());
        command.add(target.toString());
        String failure = runCopy(command);
        if (failure == null)
            return method;
        System.err.println("Copy with '" + String.join(" ", command) + "' failed, using a plain copy: " + failure);
        deleteTree(target);
        javaCopy(source, target);
        return "java (" + method + " failed)";
    }

    // Null on success, else the reason.
    private static String runCopy(List<String> command)
    {
        try
        {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            byte[] output = process.getInputStream().readAllBytes();
            if (!process.waitFor(COPY_TIMEOUT_HOURS, TimeUnit.HOURS))
            {
                process.destroyForcibly();
                return "timed out";
            }
            return process.exitValue() == 0 ? null
                                            : "exit " + process.exitValue() + ": " + new String(output, StandardCharsets.UTF_8).trim();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return "interrupted";
        }
        catch (IOException e)
        {
            return e.toString();
        }
    }

    /** Recursive copy that keeps file times. The target must not exist. */
    static String javaCopy(Path source, Path target) throws IOException
    {
        Files.walkFileTree(source, new SimpleFileVisitor<>()
        {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                Files.createDirectories(target.resolve(source.relativize(dir).toString()));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                Files.copy(file, target.resolve(source.relativize(file).toString()), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
        return "java";
    }

    static long treeBytes(Path directory) throws IOException
    {
        long[] bytes = { 0 };
        Files.walkFileTree(directory, new SimpleFileVisitor<>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            {
                bytes[0] += attrs.size();
                return FileVisitResult.CONTINUE;
            }
        });
        return bytes[0];
    }

    static void deleteTree(Path directory) throws IOException
    {
        if (!Files.exists(directory))
            return;
        try (Stream<Path> paths = Files.walk(directory))
        {
            for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator)
                Files.delete(path);
        }
    }
}
