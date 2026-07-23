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

package org.apache.cassandra.distributed.upgrade.storage.paging;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.test.storage.paging.MixedConfigPagingAcrossTombstonesTest;


@RunWith(Parameterized.class)
public class MixedVersionPagingAcrossTombstonesTest extends MixedConfigPagingAcrossTombstonesTest
{
    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> clusters() throws IOException
    {
        // VNOld == number of replicas on old version in test config
        // CMatch == config match
        // CMis == Config Mismatch -> different paging window on node
        // CDis == Config Disabled -> i.e. version turned off on N
        return Arrays.asList(new Object[][]{
        // TODO: FIX THESE
        { "V1OldCMis1", new ClusterConfig(new String[]{"4.0.5", "4.0.5", "4.0.5"},
                                            new boolean[]{true, true, true},
                                            new int[]{5, 5, 5}) },
        { "V2OldCMatch", new ClusterConfig(new String[]{"4.0.5", "4.0.5", "4.0.5"},
                                          new boolean[]{true, true, true},
                                          new int[]{5, 5, 5}) },
        { "V1OldCDis1", new ClusterConfig(new String[]{"4.0.5", "4.0.5", "4.0.5"},
                                          new boolean[]{true, true, true},
                                          new int[]{5, 5, 5}) },
        { "V1OldCMatch", new ClusterConfig(new String[]{"4.0.5", "4.0.5", "4.0.5"},
                                          new boolean[]{true, true, true},
                                          new int[]{5, 5, 5}) }
        });
    }

    public MixedVersionPagingAcrossTombstonesTest(String name, ClusterConfig config)
    {
        super(name, config);
    }
}