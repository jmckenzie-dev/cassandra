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

package org.apache.cassandra.distributed.test.schemascale;

import java.net.InetSocketAddress;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

/**
 * Sends CQL to a node over the native protocol with the Java driver (3.12.1, shaded jar). Client-side
 * schema metadata is off, so the driver does not parse the whole schema after each DDL. Timeouts are long
 * because a DDL on a large schema takes minutes. The executor outlives node restarts: call
 * {@link #disconnect} before a stop and {@link #connect} after a start.
 */
public final class DriverSchemaExecutor implements SchemaExecutor, AutoCloseable
{
    static final int CONNECT_TIMEOUT_MILLIS = 60_000;
    static final int READ_TIMEOUT_MILLIS = 600_000;
    static final int SCHEMA_AGREEMENT_WAIT_SECONDS = 600;

    private final InetSocketAddress address;
    private Cluster cluster;
    private Session session;

    public DriverSchemaExecutor(InetSocketAddress address)
    {
        this.address = address;
    }

    /** Opens one connection to the node. Throws when the node does not answer. */
    public synchronized void connect()
    {
        if (session != null)
            return;
        Cluster built = Cluster.builder()
                               .addContactPointsWithPorts(List.of(address))
                               .withLoadBalancingPolicy(new RoundRobinPolicy())
                               .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                               .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(CONNECT_TIMEOUT_MILLIS)
                                                                     .setReadTimeoutMillis(READ_TIMEOUT_MILLIS))
                               .withPoolingOptions(new PoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 1, 1)
                                                                       .setMaxRequestsPerConnection(HostDistance.LOCAL, 128))
                               .withQueryOptions(new QueryOptions().setMetadataEnabled(false)
                                                                  .setConsistencyLevel(ConsistencyLevel.ONE))
                               .withMaxSchemaAgreementWaitSeconds(SCHEMA_AGREEMENT_WAIT_SECONDS)
                               .withoutMetrics()
                               .withoutJMXReporting()
                               .build();
        try
        {
            session = built.connect();
            cluster = built;
        }
        catch (RuntimeException e)
        {
            built.close();
            throw e;
        }
    }

    /** Closes the connection. Safe to call when not connected. */
    public synchronized void disconnect()
    {
        if (cluster == null)
            return;
        try
        {
            cluster.close();
        }
        finally
        {
            cluster = null;
            session = null;
        }
    }

    @Override
    public void execute(String cql)
    {
        session().execute(new SimpleStatement(cql));
    }

    @Override
    public Object[][] query(String cql, Object... binds)
    {
        ResultSet result = session().execute(binds.length == 0 ? new SimpleStatement(cql) : new SimpleStatement(cql, binds));
        ColumnDefinitions columns = result.getColumnDefinitions();
        List<Row> rows = result.all();
        Object[][] values = new Object[rows.size()][];
        for (int i = 0; i < values.length; i++)
        {
            values[i] = new Object[columns.size()];
            for (int column = 0; column < values[i].length; column++)
                values[i][column] = rows.get(i).getObject(column);
        }
        return values;
    }

    private synchronized Session session()
    {
        if (session == null)
            connect();
        return session;
    }

    @Override
    public void close()
    {
        disconnect();
    }
}
