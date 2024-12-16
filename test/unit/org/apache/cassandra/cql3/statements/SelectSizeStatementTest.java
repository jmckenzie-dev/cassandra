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

package org.apache.cassandra.cql3.statements;

import java.util.Collections;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SelectSizeCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SelectSizeStatementTest
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        TableMetadata[] cfms = {
            CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks").build(),
            CreateTableStatement.parse("CREATE TABLE tbl2 (k1 INT, k2 INT , v INT, PRIMARY KEY ((k1, k2)))", "ks").build(),
        };
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), cfms);
    }

    @Test
    public void successCase()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare(ClientState.forInternalCalls());
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl2 WHERE k1=1 AND k2=2").prepare(ClientState.forInternalCalls());
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl WHERE k=1").prepare(ClientState.forInternalCalls());
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl2 WHERE k1=1 AND k2=2").prepare(ClientState.forInternalCalls());
    }

    /**
     * Check proper commands are created for both parameterized and un-parameterized queries
     */
    @Test
    public void commandCreation()
    {
        SelectSizeStatement statement;
        SelectSizeCommand command;

        // parameterized
        ClientState clientState = ClientState.forInternalCalls();
        statement = (SelectSizeStatement) QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=?").prepare(clientState);
        command = statement.createCommandForTest(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList(ByteBufferUtil.bytes(1))), clientState);
        Assert.assertEquals("ks", command.keyspace);
        Assert.assertEquals("tbl", command.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), command.key);

        // not parameterized
        statement = (SelectSizeStatement) QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare(clientState);
        command = statement.createCommandForTest(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList()), clientState);
        Assert.assertEquals("ks", command.keyspace);
        Assert.assertEquals("tbl", command.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), command.key);
    }

    /**
     * Non '=' where terms should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonEqOp()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k>1").prepare(ClientState.forInternalCalls());
    }

    /**
     * Specifying only part of a compound partition key should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void incompletePartitionKey()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl2 WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void nonExistantTable()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.notable WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = SyntaxException.class)
    public void noWhereClause()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl").prepare(ClientState.forInternalCalls());
    }

    /**
     * Including a non-partition key in the where clause should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonPartitionKeyWhere()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1 AND v=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void repeatedColumns()
    {
        QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1 AND k=2").prepare(ClientState.forInternalCalls());
    }

    /**
     * Attempting to execute with serial consistency should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void serialConsistency()
    {
        CQLStatement prepared = QueryProcessor.parseStatement("SELECT_SIZE FROM ks.tbl WHERE k=1").prepare(ClientState.forInternalCalls());
        Assert.assertSame(SelectSizeStatement.class, prepared.getClass());
        prepared.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(ConsistencyLevel.SERIAL, Collections.emptyList()), Dispatcher.RequestTime.forImmediateExecution());
    }

    @Test
    public void commandCreationCompressed()
    {
        testEnhancedCommandCreation("SELECT_COMPRESSED_SIZE");
    }

    @Test
    public void commandCreationLiveCalc()
    {
        testEnhancedCommandCreation("ESTIMATE_LIVE_UNCOMPRESSED_SIZE");
    }

    /**
     * Check proper commands are created for both parameterized and un-parameterized queries
     */
    private void testEnhancedCommandCreation(String cmd)
    {
        SelectSizeStatement lStatement;
        SelectSizeCommand lCommand;

        // parameterized
        ClientState clientState = ClientState.forInternalCalls();
        lStatement = (SelectSizeStatement) QueryProcessor.parseStatement(cmd + " FROM ks.tbl WHERE k=?").prepare(clientState);
        lCommand = lStatement.createCommandForTest(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList(ByteBufferUtil.bytes(1))), clientState);
        Assert.assertEquals("ks", lCommand.keyspace);
        Assert.assertEquals("tbl", lCommand.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), lCommand.key);

        // not parameterized
        lStatement = (SelectSizeStatement) QueryProcessor.parseStatement(cmd + " FROM ks.tbl WHERE k=1").prepare(clientState);
        lCommand = lStatement.createCommandForTest(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, Lists.newArrayList()), clientState);
        Assert.assertEquals("ks", lCommand.keyspace);
        Assert.assertEquals("tbl", lCommand.table);
        Assert.assertEquals(ByteBufferUtil.bytes(1), lCommand.key);
    }

    /**
     * Non '=' where terms should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonEqOpCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl WHERE k>1").prepare(ClientState.forInternalCalls());
    }

    /**
     * Non '=' where terms should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonEqOpLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl WHERE k>1").prepare(ClientState.forInternalCalls());
    }

    /**
     * Specifying only part of a compound partition key should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void incompletePartitionKeyCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl2 WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    /**
     * Specifying only part of a compound partition key should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void incompletePartitionKeyLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl2 WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void nonExistantTableCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.notable WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void nonExistantTableLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.notable WHERE k1=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = SyntaxException.class)
    public void noWhereClauseCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = SyntaxException.class)
    public void noWhereClauseLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl").prepare(ClientState.forInternalCalls());
    }

    /**
     * Including a non-partition key in the where clause should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonPartitionKeyWhereCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl WHERE k=1 AND v=1").prepare(ClientState.forInternalCalls());
    }

    /**
     * Including a non-partition key in the where clause should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void nonPartitionKeyWhereLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl WHERE k=1 AND v=1").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void repeatedColumnsCompressed()
    {
        QueryProcessor.parseStatement("SELECT_COMPRESSED_SIZE FROM ks.tbl WHERE k=1 AND k=2").prepare(ClientState.forInternalCalls());
    }

    @Test(expected = InvalidRequestException.class)
    public void repeatedColumnsLiveCalc()
    {
        QueryProcessor.parseStatement("ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM ks.tbl WHERE k=1 AND k=2").prepare(ClientState.forInternalCalls());
    }

    /**
     * Attempting to execute with serial consistency should fail
     */
    @Test(expected = InvalidRequestException.class)
    public void serialConsistencyCompressed()
    {
        testSerialConsistency("SELECT_COMPRESSED_SIZE");
    }

    @Test(expected = InvalidRequestException.class)
    public void serialConsistencyLiveCalc()
    {
        testSerialConsistency("ESTIMATE_LIVE_UNCOMPRESSED_SIZE");
    }

    private void testSerialConsistency(String cmd)
    {
        CQLStatement prepared = QueryProcessor.parseStatement(cmd + " FROM ks.tbl WHERE k=1").prepare(ClientState.forInternalCalls());
        Assert.assertSame(SelectSizeStatement.class, prepared.getClass());
        prepared.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(ConsistencyLevel.SERIAL, Collections.emptyList()), Dispatcher.RequestTime.forImmediateExecution());
    }
}
