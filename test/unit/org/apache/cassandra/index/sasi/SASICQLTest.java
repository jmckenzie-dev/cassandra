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

package org.apache.cassandra.index.sasi;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import junit.framework.Assert;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

public class SASICQLTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        // SASI Indexes are rebuilt asynchronously, so even when the compacted sstable
        // is available, index is not necessarily built.
        CompactionManager.instance.disableAutoCompaction();
    }

    // TODO: test single sstable
    // TODO: test multiple sstables
    // TODO: test very large partitions
    // TODO: test very many partitions
    // TODO: test

    @Test
    public void sasiTest() throws Throwable
    {
        int[] sizes = new int[]{ 8, 32, 128, 512, 1024, 4096 }; //

        for (int size : sizes)
        {
            for (boolean withMemtable : new boolean[]{ true, false })
            {
                createTable("CREATE TABLE %s (pk int, ck1 text, ck2 text, ck3 int, value text, PRIMARY KEY (pk, ck1, ck2, ck3))");
                createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

                for (int i = 0; i < size; i++)
                {
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 100, "aaa1", "bbb1", i, "1111");
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 100, "aaa1", "bbb2", i, "2222");
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 100, "aaa1", "bbb3", i, "3333");
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 200, "aaa1", "bbb1", i, "1111");
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 200, "aaa1", "bbb2", i, "2222");
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 200, "aaa1", "bbb3", i, "3333");
                    flush(i % (size / 2) == 0);
                }
                flush(withMemtable);
            }

            assertEquals(execute("SELECT * FROM %s WHERE value = ?", "1111").size(), size * 2);
            assertEquals(execute("SELECT * FROM %s WHERE value = ?", "2222").size(), size * 2);
            assertEquals(execute("SELECT * FROM %s WHERE value = ?", "3333").size(), size * 2);
        }
    }

    // TODO: multiple value columns

    @Test
    public void missingValuesTest() throws Throwable
    {
        int[] sizes = new int[]{ 8, 32, 128, 512, 1024, 4096 }; //

        for (int size : sizes)
        {
            System.out.println("=================================================================‚=============");
            for (boolean withMemtable : new boolean[]{ true, false })
            {
                createTable("CREATE TABLE %s (pk int PRIMARY KEY, ck1 text, ck2 text, value1 int, value2 text)");
                createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value1) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
                createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value2) USING 'org.apache.cassandra.index.sasi.SASIIndex'");

                for (int i = 0; i < size; i++)
                {
                    execute("INSERT INTO %s (pk, value1, value2) VALUES (?, ?, ?)", i, i, "1111");
                    flush(i % (size / 2) == 0);
                }
                flush(withMemtable);
            }

            assertEquals(execute("SELECT * FROM %s WHERE value1 = 1 AND value2 = ? ALLOW FILTERING", "1111").size(), 1);
//            assertEquals(execute("SELECT * FROM %s WHERE value2 = ?", "2222").size(), size * 2);
//            assertEquals(execute("SELECT * FROM %s WHERE value2 = ?", "3333").size(), size * 2);
        }
    }

    @Test
    public void sparseIndexTest() throws Throwable
    {
        int[] sizes = new int[]{ 32, 128, 512, 1024, 4096 }; //

        for (int size : sizes)
        {
            for (boolean withMemtable : new boolean[]{ true, false })
            {
                createTable("CREATE TABLE %s (pk int, ck1 text, ck2 text, ck3 text, value int, PRIMARY KEY (pk, ck1, ck2, ck3))");
                createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s(value) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = { 'mode' : 'SPARSE' }");

                for (int i = 0; i < size; i++)
                {
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 100, "aaa1", "bbb1", "ccc" + i, i);
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 100, "aaa2", "bbb2", "ccc" + i, i);
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 200, "aaa1", "bbb1", "ccc" + i, i);
                    execute("INSERT INTO %s (pk, ck1, ck2, ck3, value) VALUES (?, ?, ?, ?, ?)", 200, "aaa1", "bbb2", "ccc" + i, i);

                    execute("INSERT INTO %s (pk, ck1, ck2,  ck3, value) VALUES (?, ?, ?, ?, ?)", 300, "aaa3", "bbb3", "ccc" + i, i);
                    flush(i > 0 && i % (size / 2) == 0);
                }
                flush(withMemtable);

                assertRowCount(execute("SELECT * FROM %s WHERE value >= 0"),
                               size * 5);

                assertRowCount(execute("SELECT * FROM %s WHERE value <= " + size),
                               size * 5);

                assertRowCount(execute("SELECT * FROM %s WHERE value >= 0 AND value <= " + size),
                               size * 5);

                UntypedResultSet resultSet = execute("SELECT * FROM %s WHERE value = 0");
                assertRowsIgnoringOrder(resultSet,
                                        row(100, "aaa1", "bbb1", "ccc0", 0),
                                        row(100, "aaa2", "bbb2", "ccc0", 0),
                                        row(200, "aaa1", "bbb1", "ccc0", 0),
                                        row(200, "aaa1", "bbb2", "ccc0", 0),
                                        row(300, "aaa3", "bbb3", "ccc0", 0));

                resultSet = execute("SELECT * FROM %s WHERE value = 5");
                assertRowsIgnoringOrder(resultSet,
                                        row(100, "aaa1", "bbb1", "ccc" + 5, 5),
                                        row(100, "aaa2", "bbb2", "ccc" + 5, 5),
                                        row(200, "aaa1", "bbb1", "ccc" + 5, 5),
                                        row(200, "aaa1", "bbb2", "ccc" + 5, 5),
                                        row(300, "aaa3", "bbb3", "ccc" + 5, 5));

                int last = size - 1;
                resultSet = execute("SELECT * FROM %s WHERE value = " + last);
                assertRowsIgnoringOrder(resultSet,
                                        row(100, "aaa1", "bbb1", "ccc" + last, last),
                                        row(100, "aaa2", "bbb2", "ccc" + last, last),
                                        row(200, "aaa1", "bbb1", "ccc" + last, last),
                                        row(200, "aaa1", "bbb2", "ccc" + last, last),
                                        row(300, "aaa3", "bbb3", "ccc" + last, last));
            }
        }
    }

    @Test
    public void testPaging() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int primary key, v int);");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?);", i, 1);

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(10, rs.size());
            Assert.assertEquals(Sets.newHashSet(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                                rs.stream().map((i) -> i.getInt("pk")).collect(Collectors.toSet()));
        }
    }

    @Test
    public void testPagingWithClustering() throws Throwable
    {
        for (boolean forceFlush : new boolean[]{ false, true })
        {
            createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'org.apache.cassandra.index.sasi.SASIIndex';");

            for (int i = 0; i < 10; i++)
            {
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 1, 1);
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?);", i, 2, 1);
            }

            flush(forceFlush);

            Session session = sessionNet();
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE v = 1");
            stmt.setFetchSize(5);
            List<Row> rs = session.execute(stmt).all();
            Assert.assertEquals(20, rs.size());
        }
    }
}
