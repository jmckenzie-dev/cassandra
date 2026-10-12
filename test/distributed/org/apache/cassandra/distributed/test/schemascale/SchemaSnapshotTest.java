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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/** Saved schemas: copy and restore, the schema key and its refusal. No node starts; files go under ./tmp. */
public class SchemaSnapshotTest
{
    private static final Path TMP = Path.of("tmp", "schema-snapshot-test").toAbsolutePath();
    private static final String SCHEMA = "pk int, c int, v text, PRIMARY KEY (pk, c)";
    private static final Instant SAVED_AT = Instant.parse("2026-10-02T12:00:00Z");

    @Test
    public void fastCopyRoundTrip() throws IOException
    {
        roundTrip("fast", SchemaSnapshots::fastCopy);
    }

    @Test
    public void javaCopyRoundTrip() throws IOException
    {
        roundTrip("java", SchemaSnapshots::javaCopy);
    }

    private static void roundTrip(String name, SchemaSnapshots.Copier copier) throws IOException
    {
        Path base = clean(TMP.resolve(name));
        Path node = fakeNodeData(base.resolve("run1").resolve("node").resolve("data"));
        SchemaSnapshots store = new SchemaSnapshots(base.resolve("snapshots"), copier, () -> SAVED_AT);
        String key = SchemaSnapshots.schemaKey(SCHEMA, 1, "6.0-test");

        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("keyspaces", 1);
        facts.put("gitSha", "abc123");
        facts.put("ports", Map.of("storagePort", 7012, "nativeTransportPort", 9042));
        Map<String, Object> saved = store.save(node, key, 1000, facts);

        Path directory = store.directory(key, 1000);
        assertEquals(directory.toString(), saved.get("directory"));
        assertTrue(Files.isDirectory(directory.resolve(SchemaSnapshots.DATA)));
        assertFalse("the .partial directory is renamed", Files.exists(directory.resolveSibling("t1000.partial")));
        assertEquals(SchemaSnapshots.treeBytes(node), ((Number) saved.get("bytes")).longValue());
        assertTrue(((Number) saved.get("copyMs")).longValue() >= 0);
        assertTrue(String.valueOf(saved.get("copyMethod")).startsWith(name.equals("java") ? "java" : ""));

        Map<String, Object> manifest = SchemaSnapshots.readManifest(directory);
        assertEquals(key, manifest.get("schemaKey"));
        assertEquals(1000, ((Number) manifest.get("tables")).intValue());
        assertEquals("abc123", manifest.get("gitSha"));
        assertEquals(SAVED_AT.toString(), manifest.get("savedAt"));
        assertEquals(7012, ((Number) ((Map<?, ?>) manifest.get("ports")).get("storagePort")).intValue());

        Path snapshot = store.resolve("t1000", key);
        assertEquals(directory, snapshot);
        assertEquals(directory, store.resolve(directory.toString(), "other-key"));
        Path target = base.resolve("run2").resolve("node").resolve("data");
        Files.createDirectories(target); // an empty directory is fine
        Map<String, Object> restored = store.restore(snapshot, key, target);
        assertEquals(snapshot.toString(), restored.get("restoredFrom"));
        assertEquals(1000, ((Number) restored.get("tables")).intValue());
        assertSameTree(node, target);

        assertThatThrownBy(() -> store.save(node, key, 1000, facts)).isInstanceOf(IllegalStateException.class)
                                                                     .hasMessageContaining("exists");
        assertThatThrownBy(() -> store.restore(snapshot, key, target)).isInstanceOf(IllegalStateException.class)
                                                                       .hasMessageContaining("not empty");
        assertThatThrownBy(() -> store.resolve("t2000", key)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void aDifferentSchemaKeyIsRefused() throws IOException
    {
        Path base = clean(TMP.resolve("mismatch"));
        Path node = fakeNodeData(base.resolve("run1").resolve("node").resolve("data"));
        SchemaSnapshots store = new SchemaSnapshots(base.resolve("snapshots"), SchemaSnapshots::javaCopy, () -> SAVED_AT);
        String saved = SchemaSnapshots.schemaKey(SCHEMA, 1, "6.0-test");
        store.save(node, saved, 500, Map.of());

        String otherSpread = SchemaSnapshots.schemaKey(SCHEMA, 10, "6.0-test");
        Path target = base.resolve("run2").resolve("node").resolve("data");
        assertThatThrownBy(() -> store.restore(store.directory(saved, 500), otherSpread, target))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("schema key")
            .hasMessageContaining(saved)
            .hasMessageContaining(otherSpread);
        assertFalse("nothing is copied after a refusal", Files.exists(target));
    }

    @Test
    public void schemaKeyCoversSchemaSpreadAndVersion()
    {
        String key = SchemaSnapshots.schemaKey(SCHEMA, 1, "6.0");
        assertTrue(key, key.matches("ks1-[0-9a-f]{12}"));
        assertEquals(key, SchemaSnapshots.schemaKey("  pk int,  c int, v text,\n PRIMARY KEY (pk, c) ", 1, "6.0"));
        assertNotEquals(key, SchemaSnapshots.schemaKey(SCHEMA, 2, "6.0"));
        assertNotEquals(key, SchemaSnapshots.schemaKey(SCHEMA, 1, "6.1"));
        assertNotEquals(key, SchemaSnapshots.schemaKey("pk int PRIMARY KEY, v text", 1, "6.0"));
    }

    // The layout NodeConfigRenderer gives a process-mode node, with a few files and an empty directory.
    private static Path fakeNodeData(Path data) throws IOException
    {
        Path table = Files.createDirectories(data.resolve("data").resolve("ks_000").resolve("t000000-0123456789abcdef"));
        Files.writeString(table.resolve("nb-1-big-Data.db"), "rows", StandardCharsets.UTF_8);
        Path system = Files.createDirectories(data.resolve("data").resolve("system_cluster_metadata").resolve("distributed_metadata_log-1"));
        Files.write(system.resolve("nb-1-big-Data.db"), new byte[]{ 1, 2, 3, 4, 5 });
        Files.write(Files.createDirectories(data.resolve("commitlog")).resolve("CommitLog-8-1.log"), new byte[4096]);
        Files.createDirectories(data.resolve("saved_caches"));
        Files.createDirectories(data.resolve("hints"));
        Files.createDirectories(data.resolve("cdc_raw"));
        return data;
    }

    private static void assertSameTree(Path expected, Path actual) throws IOException
    {
        Map<String, byte[]> left = tree(expected);
        Map<String, byte[]> right = tree(actual);
        assertEquals(left.keySet(), right.keySet());
        for (Map.Entry<String, byte[]> entry : left.entrySet())
            assertArrayEquals(entry.getKey(), entry.getValue(), right.get(entry.getKey()));
    }

    // Relative path to content; directories map to an empty array.
    private static Map<String, byte[]> tree(Path root) throws IOException
    {
        Map<String, byte[]> files = new TreeMap<>();
        List<Path> paths = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root))
        {
            walk.forEach(paths::add);
        }
        for (Path path : paths)
            files.put(root.relativize(path).toString(), Files.isDirectory(path) ? new byte[0] : Files.readAllBytes(path));
        return files;
    }

    private static Path clean(Path directory) throws IOException
    {
        SchemaSnapshots.deleteTree(directory);
        return Files.createDirectories(directory);
    }
}
