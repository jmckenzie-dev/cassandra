/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.distributed.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class TombstonePagingTestUtil
{
    private TombstonePagingTestUtil()
    {
    }

    /** Reads individual native pages, bounding duplicate rows and nonterminating continuations. */
    public static List<Row> readPages(Session session, String cql, int fetchSize, int rowBudget)
    {
        List<Row> rows = new ArrayList<>();
        List<PagingState> states = new ArrayList<>();
        PagingState state = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        do
        {
            assertTrue(cql + ": page budget exceeded; states=" + states, states.size() <= rowBudget + 4);
            assertTrue(cql + ": deadline exceeded; states=" + states, System.nanoTime() < deadline);
            SimpleStatement statement = new SimpleStatement(cql);
            statement.setConsistencyLevel(ConsistencyLevel.ALL);
            statement.setFetchSize(fetchSize);
            statement.setReadTimeoutMillis(15000);
            if (state != null)
                statement.setPagingState(state);
            ResultSet page = session.execute(statement);
            state = page.getExecutionInfo().getPagingState();
            int available = page.getAvailableWithoutFetching();
            for (int i = 0; i < available; i++)
                rows.add(page.one());
            assertTrue(cql + ": row budget exceeded; states=" + states, rows.size() <= rowBudget);
            if (state != null)
            {
                assertFalse(cql + ": repeated paging state " + state, states.contains(state));
                states.add(state);
            }
        }
        while (state != null);
        return rows;
    }
}
