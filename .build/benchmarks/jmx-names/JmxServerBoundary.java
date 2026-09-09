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

import java.lang.management.ManagementFactory;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryEval;
import javax.management.QueryExp;
import javax.management.Query;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.github.jamm.MemoryMeter;

/** A deliberately incomplete query adapter, used only to test its interception boundary. */
public final class JmxServerBoundary
{
    private static final String DOMAIN = "org.apache.cassandra.metrics";
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
        ObjectName objectName();
    }

    public static final class Value implements ValueMBean, MBeanRegistration
    {
        private ObjectName registeredName;
        private MBeanServer registeredServer;

        public long getCount()
        {
            return 7;
        }

        public ObjectName objectName()
        {
            return registeredName;
        }

        public ObjectName preRegister(MBeanServer server, ObjectName name)
        {
            registeredServer = server;
            registeredName = name;
            return name;
        }

        public void postRegister(Boolean done) {}
        public void preDeregister() {}
        public void postDeregister() {}
    }

    public static final class Builder extends MBeanServerBuilder
    {
        public MBeanServer newMBeanServer(String domain, MBeanServer outer, MBeanServerDelegate delegate)
        {
            Adapter handler = new Adapter();
            MBeanServer proxy = (MBeanServer) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                                   new Class<?>[] { MBeanServer.class }, handler);
            handler.server = super.newMBeanServer(domain, outer == null ? proxy : outer, delegate);
            return proxy;
        }
    }

    private static final class Adapter implements InvocationHandler
    {
        private MBeanServer server;

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
        {
            try
            {
                String operation = method.getName();
                if (operation.equals("queryNames") || operation.equals("queryMBeans"))
                {
                    ObjectName requested = (ObjectName) args[0];
                    QueryExp expression = (QueryExp) args[1];
                    ObjectName pattern = requested == null ? ObjectName.WILDCARD : requested;
                    if (pattern.getDomain().isEmpty())
                        pattern = new ObjectName(server.getDefaultDomain() + pattern.toString());
                    ObjectName domain = new ObjectName(pattern.getDomain() + ":*");
                    ObjectName match = pattern;
                    QueryExp copies = new CopyQuery(match, expression);
                    if (operation.equals("queryNames"))
                    {
                        Set<ObjectName> result = new HashSet<>();
                        for (ObjectName name : server.queryNames(domain, copies))
                            result.add(copy(name));
                        return result;
                    }
                    Set<ObjectInstance> result = new HashSet<>();
                    for (ObjectInstance instance : server.queryMBeans(domain, copies))
                        result.add(copy(instance));
                    return result;
                }
                Object result = method.invoke(server, args);
                if (result instanceof ObjectInstance)
                    return copy((ObjectInstance) result);
                return result;
            }
            catch (InvocationTargetException e)
            {
                throw e.getCause();
            }
        }
    }

    private static class CopyQuery extends QueryEval implements QueryExp
    {
        private final ObjectName pattern;
        private final QueryExp expression;

        private CopyQuery(ObjectName pattern, QueryExp expression)
        {
            this.pattern = pattern;
            this.expression = expression;
        }

        public void setMBeanServer(MBeanServer server)
        {
            super.setMBeanServer(server);
            if (expression != null)
                expression.setMBeanServer(server);
        }

        public boolean apply(ObjectName name) throws javax.management.BadStringOperationException,
                                                    javax.management.BadBinaryOpValueExpException,
                                                    javax.management.BadAttributeValueExpException,
                                                    javax.management.InvalidApplicationException
        {
            ObjectName temporary = copy(name);
            return pattern.apply(temporary) && (expression == null || expression.apply(temporary));
        }
    }

    private static ObjectName copy(ObjectName name)
    {
        try
        {
            return new ObjectName(name.getCanonicalName());
        }
        catch (MalformedObjectNameException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    private static ObjectInstance copy(ObjectInstance instance)
    {
        return new ObjectInstance(copy(instance.getObjectName()), instance.getClassName());
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("# java=" + Runtime.version());
        if (args.length == 1 && args[0].equals("--properties"))
        {
            properties();
            return;
        }
        if (args.length == 1 && args[0].equals("--benchmark"))
        {
            benchmark();
            return;
        }
        if (args.length == 1 && args[0].equals("--late"))
        {
            MBeanServer before = ManagementFactory.getPlatformMBeanServer();
            System.setProperty("javax.management.builder.initial", Builder.class.getName());
            require(ManagementFactory.getPlatformMBeanServer() == before, "Late setting cannot replace platform server");
            require(!Proxy.isProxyClass(before.getClass()), "Existing platform server is unwrapped");
            require(Proxy.isProxyClass(MBeanServerFactory.newMBeanServer().getClass()), "New servers use late builder");
            System.out.println("# late_builder=existing_platform_unchanged,new_server_wrapped PASS");
            return;
        }
        boolean protectedServer = args.length > 0 && args[0].equals("--protected");
        int population = args.length <= (protectedServer ? 1 : 0) ? 1000 : Integer.parseInt(args[protectedServer ? 1 : 0]);
        require(population >= 2 && population <= 1000, "Require 2..1000 names");
        System.setProperty("javax.management.builder.initial", protectedServer
                           ? "org.apache.cassandra.utils.TransientMBeanServerBuilder" : Builder.class.getName());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        require(Proxy.isProxyClass(server.getClass()), "Early builder covers direct platform access");
        MemoryMeter meter = MemoryMeter.builder().build();
        List<ObjectName> persistent = new ArrayList<>();
        List<ObjectName> notifications = new ArrayList<>();
        NotificationListener listener = (notification, handback) -> {
            if (notification instanceof MBeanServerNotification)
                notifications.add(((MBeanServerNotification) notification).getMBeanName());
        };
        server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener, null, null);
        try
        {
            for (int i = 0; i < population; i++)
            {
                ObjectName name = new ObjectName(DOMAIN + ":type=Table,keyspace=heap_census,scope=t" + i + ",name=ReadLatency");
                Value bean = new Value();
                ObjectInstance result = server.registerMBean(bean, name);
                persistent.add(name);
                result.getObjectName().getKeyProperty("scope");
                require(bean.registeredName == name, "Callback retains repository name");
                require(bean.registeredServer == server, "Callback receives outer server");
            }
            require(cached(persistent) == 0, "Copies in registration results stay independent");
            long before = meter.measureDeep(persistent);
            ObjectName narrow = new ObjectName(DOMAIN + ":scope=t0,*");
            require(server.queryNames(narrow, null).size() == 1, "Narrow local query");
            require(server.queryNames(new ObjectName(DOMAIN + ":type=ThreadPools,*"), null).isEmpty(), "No-match local query");
            for (ObjectInstance instance : server.queryMBeans(new ObjectName(DOMAIN + ":*"), null))
                instance.getObjectName().getKeyPropertyList();
            for (ObjectName name : server.queryNames(new ObjectName(DOMAIN + ":*"), null))
            {
                name.getKeyPropertyList();
                server.getObjectInstance(name).getObjectName().getKeyPropertyList();
                require(server.getAttribute(name, "Count").equals(7L), "Attribute unchanged");
            }
            QueryExp inspect = new CopyQuery(new ObjectName(DOMAIN + ":*"), null)
            {
                public boolean apply(ObjectName name)
                {
                    require(Proxy.isProxyClass(QueryEval.getMBeanServer().getClass()), "Query callback receives outer server");
                    name.getKeyPropertyList();
                    return true;
                }
            };
            require(server.queryNames(narrow, inspect).size() == 1, "Expression runs only for selected name");
            remote(server, narrow);
            require(cached(persistent) == 0, "Query adapter protects observed local and remote discovery");
            System.out.printf("query_adapter,names=%d,before_graph_bytes=%d,after_graph_bytes=%d,cached=%d%n",
                              population, before, meter.measureDeep(persistent), cached(persistent));
            for (ObjectName notification : notifications)
                notification.getKeyPropertyList();
            require(cached(persistent) == (protectedServer ? 0 : population), "Delegate notification cache expectation");
            System.out.printf("notification_inspection,names=%d,after_graph_bytes=%d,cached=%d%n",
                              population, meter.measureDeep(persistent), cached(persistent));
            ObjectName operationName = new ObjectName(DOMAIN + ":type=Table,keyspace=heap_census,scope=operation,name=ReadLatency");
            Value operationBean = new Value();
            server.registerMBean(operationBean, operationName);
            try
            {
                ObjectName exposed = (ObjectName) server.invoke(operationName, "objectName", null, null);
                require((exposed == operationName) != protectedServer, "Operation result identity expectation");
                exposed.getKeyPropertyList();
                require((CACHE.get(operationName) != null) != protectedServer, "Operation cache expectation");
                System.out.println("# operation_check=PASS callback_retains_repository_name=PASS");
            }
            finally
            {
                server.unregisterMBean(operationName);
            }
            System.out.println("# boundary_probe=PASS protected=" + protectedServer);
        }
        finally
        {
            server.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener);
            for (ObjectName name : persistent)
                server.unregisterMBean(name);
        }
    }

    private static int cached(List<ObjectName> names) throws Exception
    {
        int result = 0;
        for (ObjectName name : names)
            if (CACHE.get(name) != null)
                result++;
        return result;
    }

    private static void properties() throws Exception
    {
        MBeanServerBuilder control = new MBeanServerBuilder();
        MBeanServer legacy = control.newMBeanServer("default", null, control.newMBeanServerDelegate());
        MBeanServerBuilder builder = new org.apache.cassandra.utils.TransientMBeanServerBuilder();
        MBeanServer optimized = builder.newMBeanServer("default", null, builder.newMBeanServerDelegate());
        require(optimized.equals(optimized), "Proxy equals is reflexive");
        QueryExp unchangedBinding = new QueryExp()
        {
            public void setMBeanServer(MBeanServer server) {}

            public boolean apply(ObjectName name)
            {
                return QueryEval.getMBeanServer() == null;
            }
        };
        require(legacy.queryNames(ObjectName.WILDCARD, unchangedBinding)
                      .equals(optimized.queryNames(ObjectName.WILDCARD, unchangedBinding)),
                "A custom expression controls its own QueryEval binding");
        String[] parts = { "", "simple", "a,b", "a=b", "quoted\"", "slash\\", "star*", "question?", "line\n", "\u2603", "\uD83D\uDE00", "**?***?", "\\\"" };
        String[] patterns = { "*:*", "default:*", ":*", "*:type=Table,*", "*:type=Other,*", "*:scope=\"star\\**\",*",
                              "*:scope=\"*\",*", "*:name=Read?,*", "*:name=Read*,*", "missing:*", "*:missing=*,*",
                              "*:scope=\"*\"", "*:type=Table,scope=\"*\",name=Read*", "d*:name=Read*,*" };
        int checked = 0;
        for (int seed = 0; seed < 16; seed++)
        {
            Random random = new Random(seed);
            List<ObjectName> originals = new ArrayList<>();
            for (int i = 0; i < 100; i++)
            {
                String value = parts[random.nextInt(parts.length)] + random.nextInt(1000);
                ObjectName name = new ObjectName((i % 2 == 0 ? "default" : "other") + ":type=Table,scope="
                                                 + ObjectName.quote(value) + ",name=Read" + i);
                legacy.registerMBean(new Value(), copy(name));
                optimized.registerMBean(new Value(), name);
                originals.add(name);
            }
            for (int i = 0; i < 1000; i++)
            {
                ObjectName pattern = i % 3 == 0 ? new ObjectName(patterns[random.nextInt(patterns.length)])
                                              : copy(originals.get(random.nextInt(originals.size())));
                if (i % 3 == 1)
                {
                    String scope = pattern.getKeyProperty("scope");
                    String value = random.nextBoolean() ? scope : "\"*" + scope.substring(1, scope.length() - 1) + "*\"";
                    pattern = new ObjectName((random.nextBoolean() ? "*" : "oth?r") + ":scope=" + value
                                             + (random.nextBoolean() ? ",*" : ",type=Ta?le,name=Read*"));
                }
                QueryExp expression = i % 2 == 0 ? null : Query.eq(Query.attr("Count"), Query.value(i % 5 == 0 ? 8L : 7L));
                Set<ObjectName> expected = legacy.queryNames(pattern, expression);
                Set<ObjectName> actual = optimized.queryNames(pattern, expression);
                require(expected.equals(actual), "Generated queryNames mismatch: " + pattern);
                require(legacy.queryMBeans(pattern, expression).equals(optimized.queryMBeans(pattern, expression)),
                        "Generated queryMBeans mismatch: " + pattern + " expected=" + legacy.queryMBeans(pattern, expression)
                        + " actual=" + optimized.queryMBeans(pattern, expression));
                for (ObjectName name : actual)
                    name.getKeyPropertyList();
                require(cached(originals) == 0, "Generated queries preserve cold registered names");
                checked++;
            }
            for (ObjectName name : originals)
            {
                legacy.unregisterMBean(name);
                optimized.unregisterMBean(name);
                optimized.registerMBean(new Value(), name);
                require(optimized.queryNames(name, null).size() == 1, "Recreated name is discoverable");
                optimized.unregisterMBean(name);
                require(optimized.queryNames(name, null).isEmpty(), "Dropped name is absent");
            }
        }
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ObjectName churn = new ObjectName("default:type=Table,scope=churn,name=Read");
        Thread registrar = new Thread(() -> {
            try
            {
                for (int i = 0; i < 1000; i++)
                {
                    optimized.registerMBean(new Value(), copy(churn));
                    optimized.unregisterMBean(churn);
                }
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        registrar.start();
        for (int i = 0; i < 1000; i++)
        {
            Set<ObjectName> names = optimized.queryNames(new ObjectName("default:type=Table,*"), null);
            require(names.isEmpty() || names.equals(Set.of(churn)), "Concurrent query returns a valid snapshot");
        }
        registrar.join();
        require(failure.get() == null, "Concurrent registration failure: " + failure.get());
        System.out.printf("# query_properties=PASS seeds=16 cases=%d lifecycle_names=1600 concurrent_cycles=1000%n", checked);
    }

    private static void remote(MBeanServer server, ObjectName pattern) throws Exception
    {
        RMIServerSocketFactory sockets = port -> new ServerSocket(port, 0, InetAddress.getLoopbackAddress());
        JMXConnectorServer connector = JMXConnectorServerFactory.newJMXConnectorServer(
                new JMXServiceURL("service:jmx:rmi://127.0.0.1"),
                Map.of(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, sockets), server);
        connector.start();
        try (JMXConnector client = JMXConnectorFactory.connect(connector.getAddress()))
        {
            Set<ObjectName> names = client.getMBeanServerConnection().queryNames(pattern, null);
            require(names.size() == 1, "Remote narrow query returns only one name");
            names.iterator().next().getKeyPropertyList();
            require(client.getMBeanServerConnection().queryNames(pattern, Query.eq(Query.attr("Count"), Query.value(7L))).size() == 1,
                    "Remote expression retains attribute semantics");
        }
        finally
        {
            connector.stop();
        }
    }

    private static void benchmark() throws Exception
    {
        MemoryMeter meter = MemoryMeter.builder().build();
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        require(allocation.isThreadAllocatedMemorySupported(), "Thread allocation counter is required");
        allocation.setThreadAllocatedMemoryEnabled(true);
        System.out.println("mode,query,names,iterations,cold_name_graph_bytes,warm_name_graph_bytes,bytes_per_query,ns_per_query,result_count,serialized_result_bytes");
        String[] queries = { "scope=t0,*", "type=ThreadPools,*", "keyspace=heap_census,*", "*" };
        for (boolean optimized : new boolean[] { false, true })
        {
            for (String query : queries)
            {
                MBeanServerBuilder builder = optimized ? new org.apache.cassandra.utils.TransientMBeanServerBuilder()
                                                      : new MBeanServerBuilder();
                MBeanServer server = builder.newMBeanServer("default", null, builder.newMBeanServerDelegate());
                List<ObjectName> originals = new ArrayList<>();
                for (int i = 0; i < 1000; i++)
                {
                    ObjectName name = new ObjectName(DOMAIN + ":type=Table,keyspace=heap_census,scope=t" + i + ",name=ReadLatency");
                    server.registerMBean(new Value(), name);
                    originals.add(name);
                }
                long beforeGraph = meter.measureDeep(originals);
                ObjectName pattern = new ObjectName(DOMAIN + ':' + query);
                for (int i = 0; i < 100; i++)
                    server.queryNames(pattern, null);
                long beforeAllocation = allocation.getThreadAllocatedBytes(Thread.currentThread().threadId());
                long beforeTime = System.nanoTime();
                int count = 0;
                int iterations = 500;
                for (int i = 0; i < iterations; i++)
                    count += server.queryNames(pattern, null).size();
                long elapsed = System.nanoTime() - beforeTime;
                long allocated = allocation.getThreadAllocatedBytes(Thread.currentThread().threadId()) - beforeAllocation;
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (ObjectOutputStream stream = new ObjectOutputStream(bytes))
                {
                    stream.writeObject(server.queryNames(pattern, null));
                }
                System.out.printf(java.util.Locale.ROOT, "%s,%s,1000,%d,%d,%d,%d,%d,%d,%d%n",
                                  optimized ? "transient" : "legacy", ObjectName.quote(query), iterations,
                                  beforeGraph, meter.measureDeep(originals), allocated / iterations, elapsed / iterations,
                                  count / iterations, bytes.size());
                for (ObjectName name : originals)
                    server.unregisterMBean(name);
            }
        }
        System.out.println("# query_benchmark=PASS serialized_result_excludes_rmi_protocol");
    }

    private static void require(boolean condition, String message)
    {
        if (!condition)
            throw new AssertionError(message);
    }
}
