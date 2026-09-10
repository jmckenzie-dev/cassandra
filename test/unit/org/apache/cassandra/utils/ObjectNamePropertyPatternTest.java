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
package org.apache.cassandra.utils;

import javax.management.ObjectName;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ObjectNamePropertyPatternTest
{
    @Test
    public void matchesJdkForQuotedAndEmptyValues() throws Exception
    {
        String[] patterns = { "*", "k=", "k=,*", "k=\"\"", "k=*", "k=?", "k=\"*\"", "k=\"?\"",
                              "k=\"a,b\"", "k=\"a=b\",*", "k=\"a\\\"b\"", "k=\"a\\\\b\"",
                              "k=\"\\*\"", "k=\"\\?\"", "k=\"a*?b\",*", "missing=*,*",
                              "k=*,z=", "k=*,z=,*", "prefix=*,*", "k=\"\uD83D\uDE00\"" };
        String[] candidates = { "k=", "k=,z=", "k=\"\"", "k=a", "k=\"a\"", "k=\"a,b\"",
                                "k=\"a=b\"", "k=\"a\\\"b\"", "k=\"a\\\\b\"", "k=\"\\*\"",
                                "k=\"\\?\"", "k=\"axxb\",z=", "z=,k=\"a,b\"", "prefix=1,k=",
                                "prefixLong=1", "prefix=1", "other=", "k=\"\uD83D\uDE00\"" };
        for (String properties : patterns)
        {
            ObjectName pattern = new ObjectName("test:" + properties);
            ObjectNamePropertyPattern matcher = new ObjectNamePropertyPattern(pattern);
            for (String values : candidates)
            {
                ObjectName candidate = new ObjectName("test:" + values);
                assertEquals(pattern + " against " + candidate, pattern.apply(candidate), matcher.matches(candidate));
            }
        }
    }
}
