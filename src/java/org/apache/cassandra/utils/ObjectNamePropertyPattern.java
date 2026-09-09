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

import javax.management.ObjectName;

/** Matches properties of JDK-validated names after the repository has selected matching domains. */
final class ObjectNamePropertyPattern
{
    private final String[] keys;
    private final String[] values;
    private final boolean[] wildcards;
    private final boolean allowAdditionalKeys;

    ObjectNamePropertyPattern(ObjectName pattern)
    {
        keys = pattern.getKeyPropertyList().keySet().toArray(new String[0]);
        values = new String[keys.length];
        wildcards = new boolean[keys.length];
        allowAdditionalKeys = pattern.isPropertyListPattern();
        for (int i = 0; i < keys.length; i++)
        {
            values[i] = pattern.getKeyProperty(keys[i]);
            wildcards[i] = pattern.isPropertyValuePattern(keys[i]);
        }
    }

    boolean matches(ObjectName candidate)
    {
        String canonical = candidate.getCanonicalName();
        int properties = 0;
        int matched = 0;
        int keyStart = canonical.indexOf(':') + 1;
        while (keyStart < canonical.length())
        {
            int separator = canonical.indexOf('=', keyStart);
            int valueStart = separator + 1;
            int valueEnd;
            if (valueStart < canonical.length() && canonical.charAt(valueStart) == '"')
            {
                valueEnd = valueStart + 1;
                while (canonical.charAt(valueEnd) != '"')
                {
                    if (canonical.charAt(valueEnd) == '\\')
                        valueEnd++;
                    valueEnd++;
                }
                valueEnd++;
            }
            else
            {
                valueEnd = canonical.indexOf(',', valueStart);
                if (valueEnd < 0)
                    valueEnd = canonical.length();
            }
            properties++;
            for (int i = 0; i < keys.length; i++)
            {
                if (keys[i].length() != separator - keyStart || !canonical.regionMatches(keyStart, keys[i], 0, keys[i].length()))
                    continue;
                if (wildcards[i] ? !wildcardMatches(values[i], canonical, valueStart, valueEnd)
                                 : values[i].length() != valueEnd - valueStart
                                   || !canonical.regionMatches(valueStart, values[i], 0, values[i].length()))
                    return false;
                matched++;
                break;
            }
            keyStart = valueEnd + 1;
        }
        return matched == keys.length && (allowAdditionalKeys || properties == keys.length);
    }

    // JMX matches the encoded value, including quotes and backslashes, using UTF-16 '*' and '?'.
    private static boolean wildcardMatches(String pattern, String value, int start, int end)
    {
        int patternIndex = 0;
        int valueIndex = start;
        int star = -1;
        int retry = start;
        while (valueIndex < end)
        {
            if (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*')
            {
                star = patternIndex++;
                retry = valueIndex;
            }
            else if (patternIndex < pattern.length()
                     && (pattern.charAt(patternIndex) == '?' || pattern.charAt(patternIndex) == value.charAt(valueIndex)))
            {
                patternIndex++;
                valueIndex++;
            }
            else if (star >= 0)
            {
                patternIndex = star + 1;
                valueIndex = ++retry;
            }
            else
            {
                return false;
            }
        }
        while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*')
            patternIndex++;
        return patternIndex == pattern.length();
    }
}
