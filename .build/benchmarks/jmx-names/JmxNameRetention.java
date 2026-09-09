/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerFactory;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.github.jamm.MemoryMeter;

/** Observes Java 21 name-cache retention through real local and remote JMX calls. */
public final class JmxNameRetention
{
    private static final String DOMAIN = "org.apache.cassandra.metrics";
    private static final MemoryMeter METER = MemoryMeter.builder().build();
    private static final Field CACHE;

    static
    {
        try
        {
            CACHE = ObjectName.class.getDeclaredField("_propertyList");
            CACHE.setAccessible(true);
        }
        catch (ReflectiveOperationException e)
        {
            throw new ExceptionInInitializerError(e);
        }
    }

    public interface ValueMBean
    {
        long getCount();
    }

    public static final class Value implements ValueMBean
    {
        public long getCount()
        {
            return 7;
        }
    }

    @FunctionalInterface
    private interface Monitor
    {
        void run(MBeanServer server, List<ObjectName> names) throws Exception;
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("# java=" + Runtime.version());
        if (args.length == 1 && args[0].equals("--property-only"))
        {
            properties();
            return;
        }
        int population = args.length == 0 ? 1000 : Integer.parseInt(args[0]);
        if (population < 2 || population > 1000)
            throw new IllegalArgumentException("Require 2..1000 registered names");
        System.out.println("case,names,before_graph_bytes,after_graph_bytes,added_graph_bytes,cached_registered_names");
        measure("domain-query", population, false, (server, names) -> discover(server, names.size()));
        measure("canonical-sort-attributes", population, false, (server, names) -> {
            List<ObjectName> result = discover(server, names.size());
            result.sort(Comparator.comparing(ObjectName::getCanonicalName));
            read(server, result);
        });
        measure("natural-sort", population, true, (server, names) -> Collections.sort(discover(server, names.size())));
        measure("key-property", population, true, (server, names) -> inspect(discover(server, names.size())));
        measure("missing-property", population, true, (server, names) -> {
            for (ObjectName name : discover(server, names.size()))
                require(name.getKeyProperty("missing") == null, "Missing property");
        });
        measure("property-list", population, true, (server, names) -> {
            for (ObjectName name : discover(server, names.size()))
                require(name.getKeyPropertyList().size() == 4, "Property count");
        });
        measure("property-list-string", population, false, (server, names) -> {
            for (ObjectName name : discover(server, names.size()))
                require(!name.getKeyPropertyListString().isEmpty(), "Property string");
        });
        measure("query-mbeans", population, false, (server, names) -> {
            Set<ObjectInstance> result = server.queryMBeans(new ObjectName(DOMAIN + ":*"), null);
            require(result.size() == names.size(), "MBean discovery count");
            for (ObjectInstance instance : result)
                require(server.getAttribute(instance.getObjectName(), "Count").equals(7L), "MBean value");
        });
        measure("exact-object-name-query", population, false, (server, names) ->
                require(server.queryNames(new ObjectName(names.get(0).getCanonicalName()), null).size() == 1,
                        "Exact object name lookup"));
        measure("exact-property-query", population, true, (server, names) ->
                query(server, DOMAIN + ":keyspace=heap_census,*", names.size()));
        measure("single-result-property-query", population, true, (server, names) ->
                query(server, DOMAIN + ":scope=t000000,*", 1));
        measure("no-result-property-query", population, true, (server, names) ->
                query(server, DOMAIN + ":keyspace=missing,*", 0));
        measure("thread-pools-query-on-table-domain", population, true, (server, names) ->
                query(server, DOMAIN + ":type=ThreadPools,*", 0));
        measure("value-pattern-query", population, true, (server, names) ->
                query(server, DOMAIN + ":name=Read*,*", names.size()));
        measure("attribute-expression", population, false, (server, names) ->
                require(server.queryNames(new ObjectName(DOMAIN + ":*"), Query.eq(Query.attr("Count"), Query.value(7L)))
                              .size() == names.size(), "Attribute expression count"));
        measure("get-instance-inspect", population, true, (server, names) -> {
            List<ObjectName> result = discover(server, names.size());
            for (int i = 0; i < result.size(); i++)
            {
                ObjectName original = result.get(i);
                result.set(i, ObjectName.getInstance(original));
                require(result.get(i) == original, "getInstance returns the original exact-class name");
            }
            inspect(result);
        });
        measure("new-copy-inspect-attributes", population, false, (server, names) -> {
            List<ObjectName> copies = new ArrayList<>();
            for (ObjectName original : discover(server, names.size()))
            {
                ObjectName copy = new ObjectName(original.getCanonicalName());
                require(copy != original && copy.equals(original), "Equal independent copy");
                copies.add(copy);
            }
            inspect(copies);
            read(server, copies);
        });
        measure("remote-domain-inspect-attributes", population, false, (server, names) ->
                remote(server, DOMAIN + ":*", names));
        measure("remote-exact-property-query", population, true, (server, names) ->
                remote(server, DOMAIN + ":keyspace=heap_census,*", names));
        measure("remote-value-pattern-query", population, true, (server, names) ->
                remote(server, DOMAIN + ":name=Read*,*", names));
        System.out.println("# retention_probe=PASS");
    }

    private static void measure(String label, int population, boolean expectCaches, Monitor monitor) throws Exception
    {
        MBeanServer server = MBeanServerFactory.newMBeanServer();
        List<ObjectName> names = new ArrayList<>();
        try
        {
            for (int i = 0; i < population; i++)
            {
                ObjectName name = new ObjectName(DOMAIN + ":type=Table,keyspace=heap_census,scope=t"
                                                 + String.format(java.util.Locale.ROOT, "%06d", i) + ",name=ReadLatency");
                server.registerMBean(new Value(), name);
                names.add(name);
            }
            require(cached(names) == 0, "Fresh registration cache");
            Set<ObjectName> registered = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            registered.addAll(names);
            for (ObjectName returned : discover(server, population))
                require(registered.contains(returned), "Local discovery returns registered identity");
            long before = METER.measureDeep(names);
            monitor.run(server, names);
            long after = METER.measureDeep(names);
            int caches = cached(names);
            require(caches == (expectCaches ? population : 0), label + " cache expectation: " + caches);
            monitor.run(server, names);
            require(METER.measureDeep(names) == after, label + " repeated operation graph is stable");
            System.out.printf(java.util.Locale.ROOT, "%s,%d,%d,%d,%d,%d%n",
                              label, population, before, after, after - before, caches);
        }
        finally
        {
            for (ObjectName name : names)
                server.unregisterMBean(name);
        }
    }

    private static List<ObjectName> discover(MBeanServerConnection server, int expected) throws Exception
    {
        return query(server, DOMAIN + ":*", expected);
    }

    private static List<ObjectName> query(MBeanServerConnection server, String pattern, int expected) throws Exception
    {
        List<ObjectName> result = new ArrayList<>(server.queryNames(new ObjectName(pattern), null));
        require(result.size() == expected, "Discovery count for " + pattern);
        return result;
    }

    private static void inspect(List<ObjectName> names)
    {
        Collections.sort(names);
        for (ObjectName name : names)
            require("heap_census".equals(name.getKeyProperty("keyspace")), "Keyspace value");
    }

    private static void read(MBeanServerConnection server, List<ObjectName> names) throws Exception
    {
        for (ObjectName name : names)
        {
            require(server.getMBeanInfo(name).getAttributes().length == 1, "Attribute metadata");
            require(server.getAttribute(name, "Count").equals(7L), "Attribute value");
        }
    }

    private static void remote(MBeanServer server, String pattern, List<ObjectName> registered) throws Exception
    {
        RMIServerSocketFactory sockets = port -> new ServerSocket(port, 0, InetAddress.getLoopbackAddress());
        JMXConnectorServer connector = JMXConnectorServerFactory.newJMXConnectorServer(
                new JMXServiceURL("service:jmx:rmi://127.0.0.1"),
                Map.of(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, sockets), server);
        connector.start();
        try (JMXConnector client = JMXConnectorFactory.connect(connector.getAddress()))
        {
            MBeanServerConnection connection = client.getMBeanServerConnection();
            List<ObjectName> received = query(connection, pattern, registered.size());
            Set<ObjectName> identities = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            identities.addAll(registered);
            require(new HashSet<>(received).equals(new HashSet<>(registered)), "Remote name equality");
            for (ObjectName name : received)
                require(!identities.contains(name), "Remote result is a copy");
            inspect(received);
            read(connection, received);
        }
        finally
        {
            connector.stop();
        }
    }

    private static int cached(List<ObjectName> names) throws IllegalAccessException
    {
        int count = 0;
        for (ObjectName name : names)
            if (CACHE.get(name) != null)
                count++;
        return count;
    }

    private static void properties() throws Exception
    {
        String[] parts = { "", "simple", "a,b", "a=b", "quoted\"", "slash\\", "star*", "question?", "line\n", "\u2603" };
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(seed);
            for (int step = 0; step < 1000; step++)
            {
                String value = parts[random.nextInt(parts.length)] + random.nextInt(1000);
                String other = parts[random.nextInt(parts.length)];
                String key = "k" + random.nextInt(100);
                ObjectName original = new ObjectName("probe:" + key + '=' + ObjectName.quote(value)
                                                     + ",other=" + ObjectName.quote(other));
                ObjectName copy = new ObjectName(original.getCanonicalName());
                require(original != copy && original.equals(copy), "Generated copy equality");
                require(!copy.isPattern(), "Quoted literal is not a pattern");
                require(value.equals(ObjectName.unquote(copy.getKeyProperty(key))), "Generated property value");
                require(CACHE.get(original) == null, "Copy inspection leaves original cache absent");
            }
        }
        System.out.println("# properties=PASS seeds=16 cases=16000");
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new AssertionError(message);
    }
}
