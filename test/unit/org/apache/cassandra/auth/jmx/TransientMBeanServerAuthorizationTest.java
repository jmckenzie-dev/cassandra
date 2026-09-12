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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.auth.jmx;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.MBeanServerForwarder;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.security.auth.Subject;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.CassandraPrincipal;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionDetails;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.JmxInvocationListener;
import org.apache.cassandra.utils.TransientMBeanServerBuilder;

import static org.junit.Assert.assertEquals;

public class TransientMBeanServerAuthorizationTest
{
    private static final String DOMAIN = "org.apache.cassandra.metrics";
    private static final String FIRST = DOMAIN + ":type=Table,keyspace=one,scope=t,name=Count";
    private static final String SECOND = DOMAIN + ":type=Table,keyspace=two,scope=t,name=Count";
    private static final Subject SUBJECT = new Subject(true, Collections.singleton(new CassandraPrincipal("reader")),
                                                       Collections.emptySet(), Collections.emptySet());

    public interface ValueMBean
    {
        long getCount();
    }

    public static class Value implements ValueMBean
    {
        public long getCount()
        {
            return 7;
        }
    }

    private static class LoopbackClientSockets implements RMIClientSocketFactory, Serializable
    {
        public Socket createSocket(String host, int port) throws IOException
        {
            return new Socket(InetAddress.getLoopbackAddress(), port);
        }
    }

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static class TestAuthorization extends AuthorizationProxy
    {
        TestAuthorization(Set<PermissionDetails> grants, boolean superuser, boolean enabled)
        {
            isSuperuser = role -> superuser;
            getPermissions = role -> grants;
            isAuthzRequired = () -> enabled;
            isAuthSetupComplete = () -> true;
            listener = new JmxInvocationListener() {};
        }
    }

    private static MBeanServerForwarder authorization(Set<PermissionDetails> grants, boolean superuser, boolean enabled)
    {
        return (MBeanServerForwarder) Proxy.newProxyInstance(AuthorizationProxy.class.getClassLoader(),
                                                           new Class<?>[] { MBeanServerForwarder.class },
                                                           new TestAuthorization(grants, superuser, enabled));
    }

    private static Set<PermissionDetails> grants(String... names)
    {
        Set<PermissionDetails> grants = new HashSet<>();
        for (String name : names)
            for (Permission permission : new Permission[] { Permission.DESCRIBE, Permission.SELECT })
                grants.add(new PermissionDetails("reader",
                                                 name.equals("root") ? JMXResource.root() : JMXResource.mbean(name), permission));
        return grants;
    }

    @Test
    public void localAndRemoteAuthorizationMatchesLegacy() throws Exception
    {
        for (String[] resources : new String[][] { {}, { "root" }, { FIRST }, { FIRST, SECOND },
                                                  { DOMAIN + ":keyspace=one,*" }, { DOMAIN + ":type=Table,*" },
                                                  { DOMAIN + ":keyspace=missing,*" } })
        {
            compare(grants(resources), false, true, false);
            compare(grants(resources), false, true, true);
        }
        compare(Collections.emptySet(), true, true, true);
        compare(Collections.emptySet(), false, false, true);
    }

    private static void compare(Set<PermissionDetails> grants, boolean superuser, boolean enabled, boolean remote) throws Exception
    {
        assertEquals(run(new MBeanServerBuilder(), grants, superuser, enabled, remote),
                     run(new TransientMBeanServerBuilder(), grants, superuser, enabled, remote));
    }

    private static String run(MBeanServerBuilder builder, Set<PermissionDetails> grants,
                              boolean superuser, boolean enabled, boolean remote) throws Exception
    {
        MBeanServer server = builder.newMBeanServer("default", null, builder.newMBeanServerDelegate());
        server.registerMBean(new Value(), new ObjectName(FIRST));
        server.registerMBean(new Value(), new ObjectName(SECOND));
        MBeanServerForwarder forwarder = authorization(grants, superuser, enabled);
        if (!remote)
        {
            forwarder.setMBeanServer(server);
            return Subject.doAs(SUBJECT, (PrivilegedExceptionAction<String>) () -> observations(forwarder));
        }
        RMIServerSocketFactory sockets = port -> new ServerSocket(port, 0, InetAddress.getLoopbackAddress());
        JMXAuthenticator authenticate = credentials -> SUBJECT;
        JMXConnectorServer connector = JMXConnectorServerFactory.newJMXConnectorServer(
        new JMXServiceURL("service:jmx:rmi://127.0.0.1"),
        Map.of(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, sockets,
               RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, new LoopbackClientSockets(),
               JMXConnectorServer.AUTHENTICATOR, authenticate), server);
        connector.setMBeanServerForwarder(forwarder);
        connector.start();
        try (JMXConnector client = JMXConnectorFactory.connect(connector.getAddress()))
        {
            return observations(client.getMBeanServerConnection());
        }
        finally
        {
            connector.stop();
        }
    }

    private static String observations(MBeanServerConnection connection) throws Exception
    {
        StringBuilder result = new StringBuilder();
        for (String pattern : new String[] { FIRST, SECOND, DOMAIN + ":*", DOMAIN + ":keyspace=one,*",
                                              DOMAIN + ":keyspace=missing,*", DOMAIN + ":type=ThreadPools,*" })
        {
            ObjectName name = new ObjectName(pattern);
            try
            {
                Set<ObjectName> names = connection.queryNames(name, null);
                result.append(names.stream().map(ObjectName::getCanonicalName).sorted().collect(java.util.stream.Collectors.joining(";")));
                result.append('/').append(connection.queryMBeans(name, null).size());
                if (!name.isPattern())
                    result.append('/').append(connection.getAttribute(name, "Count"));
            }
            catch (SecurityException e)
            {
                result.append("denied");
            }
            result.append('\n');
        }
        return result.toString();
    }
}
