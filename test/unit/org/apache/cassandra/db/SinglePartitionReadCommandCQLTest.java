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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class SinglePartitionReadCommandCQLTest extends ReadCommandCQLTester<SinglePartitionReadCommand>
{
    @Test
    public void namesReadsPreserveTimestampPrecedenceAcrossFlushes()
    {
        qt().withFixedSeed(TEST_SEED.getLong(18424L)).withExamples(8)
            .forAll(integers().between(2, 6), integers().between(-32, 32))
            .checkAssert((count, value) -> {
                createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
                getCurrentColumnFamilyStore().disableAutoCompaction();
                for (int c = 0; c < count; c++)
                    execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 10", c, value);
                flush();
                for (int c = 1; c < count; c += 2)
                    execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 20", c, value + 10);
                flush();
                execute("DELETE FROM %s USING TIMESTAMP 15 WHERE k = 0");
                flush();
                assertTrue(getCurrentColumnFamilyStore().getLiveSSTables().size() >= 3);

                List<Object[]> expected = new ArrayList<>();
                for (int c = 0; c < count; c++)
                {
                    if (c % 3 == 0)
                    {
                        execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?) USING TIMESTAMP 30", c, value + 20);
                        expected.add(row(c, value + 20, 30L));
                    }
                    else if (c % 2 == 1)
                        expected.add(row(c, value + 10, 20L));
                }
                String names = IntStream.rangeClosed(0, count).mapToObj(Integer::toString).collect(Collectors.joining(","));
                String query = "SELECT c, v, writetime(v) FROM %s WHERE k = 0 AND c IN (" + names + ")";
                for (int phase = 0; phase < 2; phase++)
                {
                    assertRows(execute(query), expected.toArray(new Object[0][]));
                    List<Object[]> reversed = new ArrayList<>(expected);
                    Collections.reverse(reversed);
                    assertRows(execute(query + " ORDER BY c DESC"), reversed.toArray(new Object[0][]));
                    if (phase == 0)
                        flush();
                }
            });
    }

    @Test
    public void partitionLevelDeletionTest()
    {
        createTable("CREATE TABLE %s (bucket_id TEXT,name TEXT,data TEXT,PRIMARY KEY (bucket_id, name))");
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c18', 'test', 'hello')");
        flush();
        execute("insert into %s (bucket_id, name, data) values ('8772618c9009cf8f5a5e0c19', 'test2', 'hello');");
        execute("delete from %s where bucket_id = '8772618c9009cf8f5a5e0c18'");
        flush();
        UntypedResultSet res = execute("select * from %s where bucket_id = '8772618c9009cf8f5a5e0c18' and name = 'test'");
        assertTrue(res.isEmpty());
    }

    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        assertToCQLString("SELECT * FROM %s WHERE k = 0", "SELECT * FROM %s WHERE k = 0 ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0", "SELECT * FROM %s WHERE k = 0 AND c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND (c) = (0)", "SELECT * FROM %s WHERE k = 0 AND c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0", "SELECT * FROM %s WHERE k = 0 AND c > 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0", "SELECT * FROM %s WHERE k = 0 AND c < 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0", "SELECT * FROM %s WHERE k = 0 AND c >= 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0", "SELECT * FROM %s WHERE k = 0 AND c <= 0 ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c = 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c > 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c < 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c >= 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE k = 0 AND c <= 0 AND v = 1 ALLOW FILTERING");
    }

    @Override
    protected SinglePartitionReadCommand parseCommand(String query)
    {
        return parseReadCommandGroupQueries(query).get(0);
    }
}
