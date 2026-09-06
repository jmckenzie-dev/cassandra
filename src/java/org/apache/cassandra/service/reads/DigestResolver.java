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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TombstonePageContinuation;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterables.any;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class DigestResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends ResponseResolver<E, P>
{
    private volatile Message<ReadResponse> dataResponse;

    public boolean supportsTombstonePageCheckpoints()
    {
        return command.isLimitedToOnePartition() && !command.isTopK() && !command.limits().isUnlimited()
               && (replicaPlan().readQuorum() > 1 || canResumeTombstonePage());
    }

    private boolean canResumeTombstonePage()
    {
        return command.hasTombstonePageCheckpointOwner()
               && DatabaseDescriptor.getTombstonePagingThreshold() != -1
               && !command.metadata().clusteringColumns().isEmpty();
    }

    public DigestResolver(ReadCoordinator coordinator, ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        super(coordinator, command, replicaPlan, requestTime);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(Message<ReadResponse> message)
    {
        super.preprocess(message);
        Replica replica = replicaPlan().lookup(message.from());
        if (dataResponse == null && !message.payload.isDigestResponse() && replica.isFull())
            dataResponse = message;
    }

    @VisibleForTesting
    public boolean hasTransientResponse()
    {
        return hasTransientResponse(responses.snapshot());
    }

    private boolean hasTransientResponse(Collection<Message<ReadResponse>> responses)
    {
        return any(responses,
                msg -> !msg.payload.isDigestResponse()
                        && replicaPlan().lookup(msg.from()).isTransient());
    }

    public PartitionIterator getData()
    {
        Collection<Message<ReadResponse>> responses = this.responses.snapshot();

        if (!hasTransientResponse(responses))
        {
            int count = (int) dataResponse.header.params().getOrDefault(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 0);
            if (canResumeTombstonePage())
            {
                TombstonePageContinuation continuation = count == 0 ? null : (TombstonePageContinuation) dataResponse.header.params().get(ParamType.TOMBSTONE_PAGE_CONTINUATION);
                Preconditions.checkState(count == 0 || continuation != null, "Checkpoint response has no continuation");
                command.setTombstonePageContinuation(continuation, count);
            }
            PartitionIterator data = UnfilteredPartitionIterators.filter(dataResponse.payload.makeIterator(command), command.nowInSec());
            if (count == 0)
                return data;

            // A checkpoint does not establish that the partition contains only static data.
            return Transformation.apply(data, new Transformation<RowIterator>()
            {
                protected RowIterator applyToPartition(RowIterator partition)
                {
                    if (partition.hasNext())
                        return partition;
                    partition.close();
                    return null;
                }
            });
        }
        else
        {
            // This path can be triggered only if we've got responses from full replicas and they match, but
            // transient replica response still contains data, which needs to be reconciled.
            DataResolver<E, P> dataResolver
                    = new DataResolver<>(coordinator, command, replicaPlan, NoopReadRepair.instance, requestTime);

            int tombstoneLimitedCount = (int) dataResponse.header.params().getOrDefault(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 0);
            if (tombstoneLimitedCount > 0 && !dataResolver.supportsTombstonePageCheckpoints())
            {
                Row checkpoint = Preconditions.checkNotNull(command.tombstoneLimitedRow(), "Local checkpoint response has no command cursor");
                throw new TombstoneOverwhelmingException(tombstoneLimitedCount,
                                                         command.toCQLString(),
                                                         command.metadata(),
                                                         ((SinglePartitionReadCommand) command).partitionKey(),
                                                         checkpoint.clustering());
            }

            dataResolver.preprocess(dataResponse);
            // Reconcile with transient replicas
            for (Message<ReadResponse> response : responses)
            {
                Replica replica = replicaPlan().lookup(response.from());
                if (replica.isTransient())
                    dataResolver.preprocess(response);
            }

            return dataResolver.resolve();
        }
    }

    public boolean responsesMatch()
    {
        long start = nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        Collection<Message<ReadResponse>> snapshot = responses.snapshot();
        assert snapshot.size() > 0 : "Attempted response match comparison while no responses have been received.";
        boolean checkpoint = (int) dataResponse.header.params().getOrDefault(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 0) > 0;
        TombstonePageContinuation continuation = (TombstonePageContinuation) dataResponse.header.params().get(ParamType.TOMBSTONE_PAGE_CONTINUATION);
        if (checkpoint && (!canResumeTombstonePage() || continuation == null))
            return false;
        if (snapshot.size() == 1)
            return true;

        // TODO: should also not calculate if only one full node
        for (Message<ReadResponse> message : snapshot)
        {
            if (replicaPlan().lookup(message.from()).isTransient())
                continue;

            boolean otherCheckpoint = (int) message.header.params().getOrDefault(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 0) > 0;
            if (checkpoint != otherCheckpoint
                || (checkpoint && !continuation.equals(message.header.params().get(ParamType.TOMBSTONE_PAGE_CONTINUATION))))
                return false;

            ByteBuffer newDigest = message.payload.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

        return true;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }

    public DigestResolverDebugResult[] getDigestsByEndpoint()
    {
        DigestResolverDebugResult[] ret = new DigestResolverDebugResult[responses.size()];
        for (int i = 0; i < responses.size(); i++)
        {
            Message<ReadResponse> message = responses.get(i);
            ReadResponse response = message.payload;
            String digestHex = ByteBufferUtil.bytesToHex(response.digest(command));
            ret[i] = new DigestResolverDebugResult(message.from(), digestHex, message.payload.isDigestResponse());
        }
        return ret;
    }

    public static class DigestResolverDebugResult
    {
        public InetAddressAndPort from;
        public String digestHex;
        public boolean isDigestResponse;

        private DigestResolverDebugResult(InetAddressAndPort from, String digestHex, boolean isDigestResponse)
        {
            this.from = from;
            this.digestHex = digestHex;
            this.isDigestResponse = isDigestResponse;
        }
    }
}
