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
import java.io.Reader;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Builds the cassandra.yaml of a process-mode node: the base file, then the node layout (directories,
 * loopback addresses, ports), then the profile overrides. Overrides win, as in-JVM mode applies them last.
 */
final class NodeConfigRenderer
{
    static final String LOOPBACK = "127.0.0.1";
    static final int DEFAULT_NATIVE_PORT = 9042;

    /** The ports of one node. */
    static final class Ports
    {
        final int storage;
        final int sslStorage;
        final int nativeTransport;
        final int jmx;

        Ports(int storage, int sslStorage, int nativeTransport, int jmx)
        {
            this.storage = storage;
            this.sslStorage = sslStorage;
            this.nativeTransport = nativeTransport;
            this.jmx = jmx;
        }

        /** Takes 9042 for the native port when the probe says it is free; every other port comes from the probe. */
        static Ports pick(IntSupplier freePort, IntPredicate isFree)
        {
            int nativePort = isFree.test(DEFAULT_NATIVE_PORT) ? DEFAULT_NATIVE_PORT : freePort.getAsInt();
            return new Ports(freePort.getAsInt(), freePort.getAsInt(), nativePort, freePort.getAsInt());
        }

        /** Asks the operating system for a free loopback port. */
        static int systemFreePort()
        {
            try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK)))
            {
                return socket.getLocalPort();
            }
            catch (IOException e)
            {
                throw new IllegalStateException("No free loopback port: " + e, e);
            }
        }

        static boolean systemIsFree(int port)
        {
            try (ServerSocket socket = new ServerSocket(port, 1, InetAddress.getByName(LOOPBACK)))
            {
                return true;
            }
            catch (IOException e)
            {
                return false;
            }
        }
    }

    private NodeConfigRenderer() {}

    static Map<String, Object> load(Path yaml) throws IOException
    {
        try (Reader reader = Files.newBufferedReader(yaml, StandardCharsets.UTF_8))
        {
            Map<String, Object> loaded = new Yaml().load(reader);
            return loaded == null ? new LinkedHashMap<>() : new LinkedHashMap<>(loaded);
        }
    }

    /** Returns a new map; the inputs stay unchanged. */
    static Map<String, Object> render(Map<String, Object> base, Map<String, Object> overrides, Path nodeDirectory, Ports ports)
    {
        Map<String, Object> config = new LinkedHashMap<>(base);
        Path data = nodeDirectory.toAbsolutePath().resolve("data");
        config.put("data_file_directories", new ArrayList<>(List.of(data.resolve("data").toString())));
        config.put("commitlog_directory", data.resolve("commitlog").toString());
        config.put("saved_caches_directory", data.resolve("saved_caches").toString());
        config.put("hints_directory", data.resolve("hints").toString());
        config.put("cdc_raw_directory", data.resolve("cdc_raw").toString());
        config.put("listen_address", LOOPBACK);
        config.put("rpc_address", LOOPBACK);
        config.put("storage_port", ports.storage);
        config.put("ssl_storage_port", ports.sslStorage);
        config.put("native_transport_port", ports.nativeTransport);
        Map<String, Object> seed = new LinkedHashMap<>();
        seed.put("class_name", "org.apache.cassandra.locator.SimpleSeedProvider");
        seed.put("parameters", new ArrayList<>(List.of(Map.of("seeds", LOOPBACK + ':' + ports.storage))));
        config.put("seed_provider", new ArrayList<>(List.of(seed)));
        config.putAll(overrides);
        return config;
    }

    /** Creates the data directories the config names, then writes the yaml. */
    static void write(Map<String, Object> config, Path target) throws IOException
    {
        Files.createDirectories(target.getParent());
        for (String key : List.of("commitlog_directory", "saved_caches_directory", "hints_directory", "cdc_raw_directory"))
            Files.createDirectories(Path.of((String) config.get(key)));
        for (Object directory : (List<?>) config.get("data_file_directories"))
            Files.createDirectories(Path.of((String) directory));

        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setSplitLines(false);
        try (Writer writer = Files.newBufferedWriter(target, StandardCharsets.UTF_8))
        {
            new Yaml(options).dump(config, writer);
        }
    }
}
