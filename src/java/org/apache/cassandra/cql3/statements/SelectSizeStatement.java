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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SelectSizeCommand;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ClientRequestSizeMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * Provides client with a method determining the uncompressed, compressed, and live non-tombstoned data for a given pk
 * <p>
 * syntax is: SELECT_SIZE FROM <keyspace>.<table> WHERE <partition_key>=<some_val>;
 *            SELECT_COMPRESSED_SIZE FROM <keyspace>.<table> WHERE <partition_key>=<some_val>;
 *            ESTIMATE_LIVE_UNCOMPRESSED_SIZE FROM <keyspace>.<table> WHERE <partition_key>=<some_val>;
 * <p>
 */
public class SelectSizeStatement implements CQLStatement
{
    private static final ColumnIdentifier ENDPOINT_IDENTIFIER = new ColumnIdentifier("endpoint", true);

    private final TableMetadata table;
    public final VariableSpecifications bindVariables;
    private final StatementRestrictions restrictions;
    private final Type selectSizeType;

    public enum Type
    {
        UNCOMPRESSED("SELECT_SIZE", "uncompressed size on disk (bytes)", AuditLogEntryType.SELECT_SIZE),
        COMPRESSED("SELECT_COMPRESSED_SIZE", "compressed size on disk (bytes)", AuditLogEntryType.SELECT_COMPRESSED_SIZE),
        LIVE_UNCOMPRESSED("ESTIMATE_LIVE_UNCOMPRESSED_SIZE", "estimate of non-tombstoned live uncompressed size on disk (bytes)", AuditLogEntryType.ESTIMATE_LIVE_UNCOMPRESSED_SIZE);

        public final String cmd;
        public final String columnIdentifier;
        public final AuditLogEntryType auditType;

        Type(String cmd, String columnidentifier, AuditLogEntryType auditType)
        {
            this.cmd = cmd;
            this.columnIdentifier = columnidentifier;
            this.auditType = auditType;
        }

        public ColumnIdentifier getColumnIdentifier()
        {
            return new ColumnIdentifier(columnIdentifier, true);
        }
    }

    public SelectSizeStatement(TableMetadata table,
                               VariableSpecifications bindVariables,
                               StatementRestrictions restrictions,
                               Type selectSizeType)
    {
        this.table = table;
        this.bindVariables = bindVariables;
        this.restrictions = restrictions;
        this.selectSizeType = selectSizeType;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // no bespoke validation needed
    }

    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(selectSizeType.auditType, keyspace(), table.name);
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead();

        ResultMessage.Rows rows = executeInternal(state, options, requestTime);
        ClientRequestSizeMetrics.recordReadResponseMetrics(rows, null, null);
        return rows;
    }

    public ResultMessage.Rows executeLocally(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        return executeInternal(state, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    @VisibleForTesting
    SelectSizeCommand createCommandForTest(QueryOptions options, ClientState state) throws InvalidRequestException
    {
        return new SelectSizeCommand(table.keyspace, table.name, getKey(options, state), selectSizeType);
    }

    public void authorize(ClientState state)
    {
        if (table.isView())
        {
            TableMetadataRef baseTable = View.findBaseTable(keyspace(), columnFamily());
            if (baseTable != null)
                state.ensureTablePermission(baseTable, Permission.SELECT);
        }
        else
        {
            state.ensureTablePermission(table, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensurePermission(Permission.EXECUTE, function);
    }

    private ByteBuffer getKey(QueryOptions options, ClientState state) throws InvalidRequestException
    {
        List<ByteBuffer> keys = restrictions.getPartitionKeys(options, state);
        checkTrue(keys.size() == 1, String.format("%s statements can only be restricted to a single partition key.", selectSizeType.cmd));
        return keys.get(0);
    }

    private ResultMessage.Rows executeInternal(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime) throws RequestValidationException, RequestExecutionException
    {
        if (options.getConsistency().isSerialConsistency())
        {
            throw new InvalidRequestException(String.format("%s statements cannot use SERIAL consistency", selectSizeType.cmd));
        }
        SelectSizeCommand toExecute = new SelectSizeCommand(table.keyspace, table.name, getKey(options, state.getClientState()), selectSizeType);
        Map<InetAddressAndPort, Long> sizes = StorageProxy.fetchPartitionSize(toExecute, options.getConsistency(), requestTime);
        return convertToRowsResultSet(sizes);
    }

    private ResultMessage.Rows convertToRowsResultSet(Map<InetAddressAndPort, Long> sizesPerReplica)
    {
        Set<Map.Entry<InetAddressAndPort, Long>> resultEntries = sizesPerReplica.entrySet();
        List<List<ByteBuffer>> rows = new ArrayList<>(resultEntries.size());
        for (Map.Entry<InetAddressAndPort, Long> entry : resultEntries)
        {
            rows.add(getSerializedRowEntry(entry));
        }
        return new ResultMessage.Rows(new ResultSet(new ResultSet.ResultMetadata(getColumnSpecifications()), rows));
    }

    private List<ByteBuffer> getSerializedRowEntry(Map.Entry<InetAddressAndPort, Long> entry)
    {
        List<ByteBuffer> row = new ArrayList<>(2);
        row.add(InetAddressSerializer.instance.serialize(entry.getKey().getAddress()));
        row.add(LongSerializer.instance.serialize(entry.getValue()));
        return row;
    }

    private List<ColumnSpecification> getColumnSpecifications()
    {
        List<ColumnSpecification> specifications = new ArrayList<>(2);
        specifications.add(new ColumnSpecification(table.keyspace, table.name, ENDPOINT_IDENTIFIER, InetAddressType.instance));
        specifications.add(new ColumnSpecification(table.keyspace, table.name, selectSizeType.getColumnIdentifier(), LongType.instance));
        return specifications;
    }

    private String keyspace()
    {
        return table.keyspace;
    }

    private String columnFamily()
    {
        return table.name;
    }

    public static class RawStatement extends QualifiedStatement
    {
        private final WhereClause whereClause;
        private final Type selectSizeType;

        public RawStatement(QualifiedName cfName,
                            WhereClause whereClause,
                            Type selectSizeType)
        {
            super(cfName);
            this.whereClause = whereClause;
            this.selectSizeType = selectSizeType;
        }

        public SelectSizeStatement prepare(ClientState state) throws RequestValidationException
        {
            TableMetadata table = Schema.instance.validateTable(super.keyspace(), name());
            checkTrue(!table.isView(), String.format("%s statement can not be used on views", selectSizeType.cmd));

            StatementRestrictions restrictions = parseAndValidateRestrictions(state, table, bindVariables);
            return new SelectSizeStatement(table, bindVariables, restrictions, selectSizeType);
        }

        private StatementRestrictions parseAndValidateRestrictions(ClientState state, TableMetadata table, VariableSpecifications boundNames)
        {
            StatementRestrictions restrictions = new StatementRestrictions(state, StatementType.SELECT, table, whereClause, boundNames, Collections.emptyList(), false, false, false, false);
            // The WHERE clause can only restrict the query to a single partition (nothing more restrictive, nothing less restrictive).
            checkTrue(restrictions.hasPartitionKeyRestrictions(), String.format("%s statements must be restricted to a partition.", selectSizeType.cmd));
            checkTrue(!restrictions.hasClusteringColumnsRestrictions(), String.format("%s statements can only have partition key restrictions.", selectSizeType.cmd));
            checkTrue(!restrictions.hasNonPrimaryKeyRestrictions(), String.format("%s statements can only have partition key restrictions.", selectSizeType.cmd));
            return restrictions;
        }
    }
}

