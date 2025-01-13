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
package org.apache.cassandra.service;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GCInspectorTest
{
    
    GCInspector gcInspector;
    
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    @Before
    public void before()
    {
        gcInspector = new GCInspector();
    }
    
    @Test
    public void ensureStaticFieldsHydrateFromConfig()
    {
        if (CassandraRelevantProperties.JAVA_VERSION.getString().compareTo("21") > 0)
        {
            Assert.assertEquals(DatabaseDescriptor.getZGCLogThreshold(), gcInspector.getGcLogThresholdInMs());
            Assert.assertEquals(DatabaseDescriptor.getZGCWarnThreshold(), gcInspector.getGcWarnThresholdInMs());
        }
        else
        {
            Assert.assertEquals(DatabaseDescriptor.getGCLogThreshold(), gcInspector.getGcLogThresholdInMs());
            Assert.assertEquals(DatabaseDescriptor.getGCWarnThreshold(), gcInspector.getGcWarnThresholdInMs());
        }
    }
    
    @Test
    public void ensureStatusIsCalculated()
    {
        Assert.assertTrue(gcInspector.getStatusThresholdInMs() > 0);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void ensureWarnGreaterThanLog()
    {
        gcInspector.setGcWarnThresholdInMs(gcInspector.getGcLogThresholdInMs());
    }
    
    @Test
    public void ensureZeroIsOk()
    {
        gcInspector.setGcWarnThresholdInMs(0);
        Assert.assertEquals(gcInspector.getStatusThresholdInMs(), gcInspector.getGcLogThresholdInMs());
        Assert.assertEquals(0, DatabaseDescriptor.getGCWarnThreshold());
        Assert.assertEquals(200, DatabaseDescriptor.getGCLogThreshold());
        Assert.assertEquals(20000, DatabaseDescriptor.getZGCLogThreshold());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void ensureLogLessThanWarn()
    {
        Assert.assertEquals(20000, gcInspector.getGcLogThresholdInMs());
        gcInspector.setGcWarnThresholdInMs(1000);
        Assert.assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
        gcInspector.setGcLogThresholdInMs(gcInspector.getGcWarnThresholdInMs() + 1);
    }
    
    @Test
    public void testDefaults()
    {
        gcInspector.setGcLogThresholdInMs(200);
        gcInspector.setGcWarnThresholdInMs(1000);
        gcInspector.setGcPauseLogThresholdInMs(5);
        gcInspector.setGcPauseWarnThresholdInMs(10);
        Assert.assertEquals(200, DatabaseDescriptor.getGCLogThreshold());
        Assert.assertEquals(200, gcInspector.getGcLogThresholdInMs());
        Assert.assertEquals(1000, DatabaseDescriptor.getGCWarnThreshold());
        Assert.assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
        Assert.assertEquals(5, DatabaseDescriptor.getGCPauseLogThreshold());
        Assert.assertEquals(5, gcInspector.getGcPauseLogThresholdInMs());
        Assert.assertEquals(10, DatabaseDescriptor.getGCPauseWarnThreshold());
        Assert.assertEquals(10, gcInspector.getGcPauseWarnThresholdInMs());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMaxValue()
    {
        gcInspector.setGcLogThresholdInMs(200);
        gcInspector.setGcWarnThresholdInMs(Integer.MAX_VALUE+1L);
    }
}
