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

package org.apache.cassandra.metrics;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.ReflectionException;
import javax.management.StandardMBean;

/** Uses a temporary JDK adapter per call instead of retaining one for each metric registration. */
interface TransientMetricMBean extends DynamicMBean
{
    StandardMBean standardView();

    @Override
    default Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException
    {
        return standardView().getAttribute(attribute);
    }

    @Override
    default void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
                                                         MBeanException, ReflectionException
    {
        standardView().setAttribute(attribute);
    }

    @Override
    default AttributeList getAttributes(String[] attributes)
    {
        return standardView().getAttributes(attributes);
    }

    @Override
    default AttributeList setAttributes(AttributeList attributes)
    {
        return standardView().setAttributes(attributes);
    }

    @Override
    default Object invoke(String operation, Object[] parameters, String[] signature) throws MBeanException, ReflectionException
    {
        return standardView().invoke(operation, parameters, signature);
    }
}
