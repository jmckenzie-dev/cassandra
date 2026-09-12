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

package org.apache.cassandra.utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.BadAttributeValueExpException;
import javax.management.BadBinaryOpValueExpException;
import javax.management.BadStringOperationException;
import javax.management.DynamicMBean;
import javax.management.InvalidApplicationException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerDelegateMBean;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.StandardMBean;

/**
 * Keeps property matching and local discovery from populating registered ObjectName caches.
 * Select with {@code -Djavax.management.builder.initial=org.apache.cassandra.utils.TransientMBeanServerBuilder}
 * before the platform server initializes. The underlying JDK server still owns registrations.
 * MBeans that retain and inspect their own registration names can still populate those names' caches.
 */
public final class TransientMBeanServerBuilder extends MBeanServerBuilder
{
    @Override
    public MBeanServerDelegate newMBeanServerDelegate()
    {
        return new CopyingDelegate(super.newMBeanServerDelegate());
    }

    @Override
    public MBeanServer newMBeanServer(String domain, MBeanServer outer, MBeanServerDelegate delegate)
    {
        Handler handler = new Handler();
        MBeanServer proxy = (MBeanServer) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                               new Class<?>[] { MBeanServer.class }, handler);
        handler.server = super.newMBeanServer(domain, outer == null ? proxy : outer, delegate);
        return proxy;
    }

    private static ObjectName copy(ObjectName name)
    {
        try
        {
            return new ObjectName(name.toString());
        }
        catch (MalformedObjectNameException e)
        {
            throw new IllegalArgumentException("Invalid ObjectName representation", e);
        }
    }

    private static ObjectInstance copy(ObjectInstance instance)
    {
        return new ObjectInstance(copy(instance.getObjectName()), instance.getClassName());
    }

    private static final class Handler implements InvocationHandler
    {
        private MBeanServer server;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
        {
            try
            {
                String operation = method.getName();
                if (method.getDeclaringClass() == Object.class)
                {
                    if (operation.equals("equals"))
                        return proxy == args[0];
                    if (operation.equals("hashCode"))
                        return System.identityHashCode(proxy);
                }
                if (operation.equals("queryNames") || operation.equals("queryMBeans"))
                    return query(operation.equals("queryNames"), (ObjectName) args[0], (QueryExp) args[1]);

                Object result = method.invoke(server, args);
                if (result instanceof ObjectInstance)
                    return copy((ObjectInstance) result);
                if (result instanceof ObjectName)
                    return copy((ObjectName) result);
                return result;
            }
            catch (InvocationTargetException e)
            {
                throw e.getCause();
            }
        }

        private Object query(boolean namesOnly, ObjectName requested, QueryExp expression) throws MalformedObjectNameException
        {
            ObjectName pattern = requested == null ? ObjectName.WILDCARD : ObjectName.getInstance(requested);
            if (pattern.getDomain().isEmpty())
                pattern = new ObjectName(server.getDefaultDomain() + pattern.toString());

            ObjectName selection = pattern;
            QueryExp filter = expression == null ? null : new CopyingQuery(null, expression);
            if (pattern.isPattern() && !pattern.getKeyPropertyListString().isEmpty())
            {
                selection = new ObjectName(pattern.getDomain() + ":*");
                filter = new CopyingQuery(pattern, expression);
            }

            if (namesOnly)
            {
                Set<ObjectName> result = new HashSet<>();
                for (ObjectName name : server.queryNames(selection, filter))
                    result.add(copy(name));
                return result;
            }

            Set<ObjectInstance> result = new HashSet<>();
            for (ObjectInstance instance : server.queryMBeans(selection, filter))
                result.add(copy(instance));
            return result;
        }
    }

    private static final class CopyingQuery implements QueryExp
    {
        private static final long serialVersionUID = 1L;
        private final ObjectNamePropertyPattern pattern;
        private final QueryExp expression;

        private CopyingQuery(ObjectName pattern, QueryExp expression)
        {
            this.pattern = pattern == null ? null : new ObjectNamePropertyPattern(pattern);
            this.expression = expression;
        }

        @Override
        public void setMBeanServer(MBeanServer server)
        {
            if (expression != null)
                expression.setMBeanServer(server);
        }

        @Override
        public boolean apply(ObjectName name) throws BadStringOperationException, BadBinaryOpValueExpException,
                                                     BadAttributeValueExpException, InvalidApplicationException
        {
            return (pattern == null || pattern.matches(name)) && (expression == null || expression.apply(copy(name)));
        }
    }

    private static final class CopyingDelegate extends MBeanServerDelegate implements DynamicMBean, MBeanRegistration
    {
        private final MBeanServerDelegate delegate;
        private final DynamicMBean attributes;

        private CopyingDelegate(MBeanServerDelegate delegate)
        {
            this.delegate = delegate;
            attributes = delegate instanceof DynamicMBean
                         ? (DynamicMBean) delegate : new StandardMBean(delegate, MBeanServerDelegateMBean.class, false);
        }

        @Override
        public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception
        {
            return delegate instanceof MBeanRegistration ? ((MBeanRegistration) delegate).preRegister(server, name) : name;
        }

        @Override
        public void postRegister(Boolean done)
        {
            if (delegate instanceof MBeanRegistration)
                ((MBeanRegistration) delegate).postRegister(done);
        }

        @Override
        public void preDeregister() throws Exception
        {
            if (delegate instanceof MBeanRegistration)
                ((MBeanRegistration) delegate).preDeregister();
        }

        @Override
        public void postDeregister()
        {
            if (delegate instanceof MBeanRegistration)
                ((MBeanRegistration) delegate).postDeregister();
        }

        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException
        {
            return attributes.getAttribute(attribute);
        }

        @Override
        public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
                                                             MBeanException, ReflectionException
        {
            attributes.setAttribute(attribute);
        }

        @Override
        public AttributeList getAttributes(String[] names)
        {
            return attributes.getAttributes(names);
        }

        @Override
        public AttributeList setAttributes(AttributeList values)
        {
            return attributes.setAttributes(values);
        }

        @Override
        public Object invoke(String operation, Object[] parameters, String[] signature) throws MBeanException, ReflectionException
        {
            return attributes.invoke(operation, parameters, signature);
        }

        @Override
        public MBeanInfo getMBeanInfo()
        {
            return attributes.getMBeanInfo();
        }

        @Override
        public String getImplementationName()
        {
            return delegate.getImplementationName();
        }

        @Override
        public String getMBeanServerId()
        {
            return delegate.getMBeanServerId();
        }

        @Override
        public String getSpecificationName()
        {
            return delegate.getSpecificationName();
        }

        @Override
        public String getSpecificationVendor()
        {
            return delegate.getSpecificationVendor();
        }

        @Override
        public String getSpecificationVersion()
        {
            return delegate.getSpecificationVersion();
        }

        @Override
        public String getImplementationVendor()
        {
            return delegate.getImplementationVendor();
        }

        @Override
        public String getImplementationVersion()
        {
            return delegate.getImplementationVersion();
        }

        @Override
        public void sendNotification(Notification notification)
        {
            if (notification instanceof MBeanServerNotification)
            {
                MBeanServerNotification original = (MBeanServerNotification) notification;
                MBeanServerNotification independent = new MBeanServerNotification(original.getType(), original.getSource(),
                                                                                  original.getSequenceNumber(),
                                                                                  copy(original.getMBeanName()));
                independent.setTimeStamp(original.getTimeStamp());
                independent.setUserData(original.getUserData());
                notification = independent;
            }
            super.sendNotification(notification);
        }
    }
}
