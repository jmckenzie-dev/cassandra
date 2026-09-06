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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TombstonePageContinuation;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.locator.ReplicaUtils.trans;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DigestResolverTest extends AbstractReadResponseTest
{
    private static PartitionUpdate.Builder update(TableMetadata metadata, String key, Row... rows)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk(key), metadata.regularAndStaticColumns(), rows.length, false);
        for (Row row: rows)
        {
            builder.add(row);
        }
        return builder;
    }

    private static PartitionUpdate.Builder update(Row... rows)
    {
        return update(cfm, "key1", rows);
    }

    private static Row row(long timestamp, int clustering, int value)
    {
        SimpleBuilders.RowBuilder builder = new SimpleBuilders.RowBuilder(cfm, Integer.toString(clustering));
        builder.timestamp(timestamp).add("c1", Integer.toString(value));
        return builder.build();
    }

    @Test
    public void noRepairNeeded()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        DigestResolver resolver = new DigestResolver(ReadCoordinator.DEFAULT, command, plan(ConsistencyLevel.QUORUM, targetReplicas), new Dispatcher.RequestTime(0L, 0L));

        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(response), true));
        resolver.preprocess(response(command, EP1, iter(response), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertTrue(resolver.responsesMatch());

        assertPartitionsEqual(filter(iter(response)), resolver.getData());
    }

    @Test
    public void tombstonePageCheckpointFailsDigestResolverRead()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                         .withUpdatedLimit(DataLimits.cqlLimits(1));
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targetReplicas);
        Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(0L, 0L);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
        new ReadCallback<>(resolver,
                           command,
                           plan,
                           requestTime,
                           ReadCallback.TombstonePageCheckpointPolicy.USE_LOCAL_COMMAND);
        PartitionUpdate response = update(row(1000, 4, 4)).build();

        callback.onResponse(response(command, EP1, iter(response))
                            .withParam(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 3)
                            .withParam(ParamType.TOMBSTONE_FAIL, 3));

        CoordinatorWarnings.init();
        try
        {
            assertThatThrownBy(callback::awaitResults).isInstanceOf(ReadFailureException.class);
            Assert.assertFalse(resolver.isDataPresent());
        }
        finally
        {
            CoordinatorWarnings.done();
        }
    }

    @Test
    public void checkpointAgreementChecksContentBoundaryAndCompletion()
    {
        int originalThreshold = DatabaseDescriptor.getTombstonePagingThreshold();
        try
        {
            DatabaseDescriptor.setTombstonePagingThreshold(2);
            for (int variant = 0; variant < 7; variant++)
            {
                SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                                 .withUpdatedLimit(DataLimits.cqlLimits(1));
                if (variant != 5)
                    command.setTombstonePageCheckpointOwner();
                EndpointsForToken replicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
                ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, replicas);
                Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
                DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
                new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
                ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
                new ReadCallback<>(resolver, command, plan, requestTime, ReadCallback.TombstonePageCheckpointPolicy.COMPARE);
                Row data = row(1000, 4, 4);
                TombstonePageContinuation continuation = new TombstonePageContinuation(data.clustering());
                Message<ReadResponse> first = response(command, EP1, iter(update(data).build()), false)
                                             .withParam(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 3)
                                             .withParam(ParamType.TOMBSTONE_FAIL, 3);
                if (variant != 6)
                    first = first.withParam(ParamType.TOMBSTONE_PAGE_CONTINUATION, continuation);
                Message<ReadResponse> second = response(command, EP2, iter(update(row(1000, 4, variant == 1 ? 99 : 4)).build()), true);
                if (variant != 3)
                {
                    second = second.withParam(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 7).withParam(ParamType.TOMBSTONE_FAIL, 7);
                    if (variant != 4)
                        second = second.withParam(ParamType.TOMBSTONE_PAGE_CONTINUATION,
                                                  variant == 2 ? new TombstonePageContinuation(row(1000, 5, 5).clustering()) : continuation);
                }
                callback.onResponse(first);
                callback.onResponse(second);
                callback.awaitResults();
                Assert.assertEquals("variant=" + variant, variant == 0, resolver.responsesMatch());
                if (variant == 0)
                {
                    assertPartitionsEqual(filter(iter(update(data).build())), resolver.getData());
                    Assert.assertEquals(data.clustering(), command.tombstoneLimitedRow().clustering());
                    Assert.assertEquals(3, command.tombstoneLimitedCount());
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setTombstonePagingThreshold(originalThreshold);
        }
    }

    @Test
    public void legacyCheckpointAtOneStillFailsSafely()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                         .withUpdatedLimit(DataLimits.cqlLimits(1));
        command.setTombstonePageCheckpointOwner();
        EndpointsForToken replicas = EndpointsForToken.of(dk.getToken(), full(EP1));
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, replicas);
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
        new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
        new ReadCallback<>(resolver, command, plan, requestTime, ReadCallback.TombstonePageCheckpointPolicy.COMPARE);
        CoordinatorWarnings.init();
        try
        {
            callback.onResponse(response(command, EP1, iter(update(row(1000, 4, 4)).build()))
                                .withParam(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 3)
                                .withParam(ParamType.TOMBSTONE_FAIL, 3));
            assertThatThrownBy(callback::awaitResults).isInstanceOf(ReadFailureException.class);
        }
        finally
        {
            CoordinatorWarnings.done();
        }
    }

    @Test
    public void comparisonDoesNotSuppressOrdinaryTombstoneFailure()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                         .withUpdatedLimit(DataLimits.cqlLimits(1));
        EndpointsForToken replicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, replicas);
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
        new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
        new ReadCallback<>(resolver, command, plan, requestTime, ReadCallback.TombstonePageCheckpointPolicy.COMPARE);
        PartitionUpdate data = update(row(1000, 4, 4)).build();
        CoordinatorWarnings.init();
        try
        {
            callback.onResponse(response(command, EP1, iter(data)).withParam(ParamType.TOMBSTONE_FAIL, 3));
            callback.onResponse(response(command, EP2, iter(data)));
            assertThatThrownBy(callback::awaitResults).isInstanceOf(ReadFailureException.class);
        }
        finally
        {
            CoordinatorWarnings.done();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void tombstonePageCheckpointAcceptedByShortReadCapableDataResolver()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                         .withUpdatedLimit(DataLimits.cqlLimits(1));
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
        Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(0L, 0L);
        DataResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
        new DataResolver<>(ReadCoordinator.DEFAULT,
                           command,
                           plan,
                           (NoopReadRepair<EndpointsForToken, ReplicaPlan.ForTokenRead>) NoopReadRepair.instance,
                           requestTime);
        Assert.assertTrue(resolver.supportsTombstonePageCheckpoints());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
        new ReadCallback<>(resolver,
                           command,
                           plan,
                           requestTime,
                           ReadCallback.TombstonePageCheckpointPolicy.ACCEPT);
        PartitionUpdate response = update(row(1000, 4, 4)).build();

        callback.onResponse(response(command, EP1, iter(response))
                            .withParam(ParamType.TOMBSTONE_PAGE_CHECKPOINT, 3)
                            .withParam(ParamType.TOMBSTONE_FAIL, 3));
        callback.onResponse(response(command, EP2, iter(response)));
        callback.awaitResults();

        Assert.assertEquals(2, resolver.getMessages().size());
        Assert.assertTrue(resolver.getMessages().get(0).header.params().containsKey(ParamType.TOMBSTONE_PAGE_CHECKPOINT));
    }

    /**
     * This test makes a time-boxed effort to reproduce the issue found in CASSANDRA-16807.
     */
    @Test
    public void multiThreadedNoRepairNeededReadCallback()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        PartitionUpdate response = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targetReplicas);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        long endTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);

        try
        {
            while (System.nanoTime() < endTime)
            {
                final long startNanos = System.nanoTime();
                final Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(startNanos, startNanos);
                final DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
                final ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = new ReadCallback<>(resolver, command, plan, requestTime);
                
                final CountDownLatch startlatch = new CountDownLatch(2);

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.onResponse(response(command, EP1, iter(response), true));
                             });

                pool.execute(() ->
                             {
                                 startlatch.countDown();
                                 waitForLatch(startlatch);
                                 callback.onResponse(response(command, EP2, iter(response), true));
                             });

                callback.awaitResults();
                Assert.assertTrue(resolver.isDataPresent());
                Assert.assertTrue(resolver.responsesMatch());
            }
        }
        finally
        {
            pool.shutdown();
        }
    }

    @Test
    public void digestMismatch()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2));
        DigestResolver resolver = new DigestResolver(ReadCoordinator.DEFAULT, command, plan(ConsistencyLevel.QUORUM, targetReplicas), new Dispatcher.RequestTime(0L, 0L));

        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(2000, 4, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(response1), true));
        resolver.preprocess(response(command, EP1, iter(response2), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertFalse(resolver.responsesMatch());
        Assert.assertFalse(resolver.hasTransientResponse());
    }

    /**
     * A full response and a transient response, with the transient response being a subset of the full one
     */
    @Test
    public void agreeingTransient()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), trans(EP2));
        DigestResolver<?, ?> resolver = new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan(ConsistencyLevel.QUORUM, targetReplicas), new Dispatcher.RequestTime(0L, 0L));

        PartitionUpdate response1 = update(row(1000, 4, 4), row(1000, 5, 5)).build();
        PartitionUpdate response2 = update(row(1000, 5, 5)).build();

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP1, iter(response1), false));
        resolver.preprocess(response(command, EP2, iter(response2), false));
        Assert.assertTrue(resolver.isDataPresent());
        Assert.assertTrue(resolver.responsesMatch());
        Assert.assertTrue(resolver.hasTransientResponse());
    }

    /**
     * Transient responses shouldn't be classified as the single dataResponse
     */
    @Test
    public void transientResponse()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), trans(EP2));
        DigestResolver<?, ?> resolver = new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan(ConsistencyLevel.QUORUM, targetReplicas), new Dispatcher.RequestTime(0L, 0L));

        PartitionUpdate response2 = update(row(1000, 5, 5)).build();
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertFalse(resolver.hasTransientResponse());
        resolver.preprocess(response(command, EP2, iter(response2), false));
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertTrue(resolver.hasTransientResponse());
    }

    @Test
    public void transientResponseData()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2), trans(EP3));
        DigestResolver<?, ?> resolver = new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan(ConsistencyLevel.QUORUM, targetReplicas), new Dispatcher.RequestTime(0L, 0L));

        PartitionUpdate fullResponse = update(row(1000, 1, 1)).build();
        PartitionUpdate digestResponse = update(row(1000, 1, 1)).build();
        PartitionUpdate transientResponse = update(row(1000, 2, 2)).build();
        Assert.assertFalse(resolver.isDataPresent());
        Assert.assertFalse(resolver.hasTransientResponse());
        resolver.preprocess(response(command, EP1, iter(fullResponse), false));
        Assert.assertTrue(resolver.isDataPresent());
        resolver.preprocess(response(command, EP2, iter(digestResponse), true));
        resolver.preprocess(response(command, EP3, iter(transientResponse), false));
        Assert.assertTrue(resolver.hasTransientResponse());

        assertPartitionsEqual(filter(iter(dk,
                                          row(1000, 1, 1),
                                          row(1000, 2, 2))),
                              resolver.getData());
    }

    @Test
    public void localCheckpointMetadataReachesTransientReconciliation()
    {
        int originalFailureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
        CoordinatorWarnings.init();
        try
        {
            cfs.truncateBlocking();
            DatabaseDescriptor.setTombstoneFailureThreshold(2);
            RowUpdateBuilder.deleteRow(cfm, 20, ByteBufferUtil.bytes("key1"), "0").applyUnsafe();
            RowUpdateBuilder.deleteRow(cfm, 20, ByteBufferUtil.bytes("key1"), "1").applyUnsafe();
            RowUpdateBuilder.deleteRow(cfm, 20, ByteBufferUtil.bytes("key1"), "2").applyUnsafe();
            new RowUpdateBuilder(cfm, 10, ByteBufferUtil.bytes("key1")).clustering("3").add("c1", "3").build().applyUnsafe();
            new RowUpdateBuilder(cfm, 10, ByteBufferUtil.bytes("key1")).clustering("4").add("c1", "4").build().applyUnsafe();

            SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk)
                                                                             .withUpdatedLimit(DataLimits.cqlLimits(2));
            command.setTombstonePageCheckpointOwner();
            EndpointsForToken targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), trans(EP2));
            ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
            Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
            DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver =
            new DigestResolver<>(ReadCoordinator.DEFAULT, command, plan, requestTime);
            ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback =
            new ReadCallback<>(resolver,
                               command,
                               plan,
                               requestTime,
                               ReadCallback.TombstonePageCheckpointPolicy.USE_LOCAL_COMMAND);

            try (ReadExecutionController controller = command.executionController();
                 UnfilteredPartitionIterator iterator = command.executeLocally(controller, 2))
            {
                ReadResponse localResponse = ReadResponse.createDataResponse(iterator, command);
                Assert.assertEquals(3, command.tombstoneLimitedCount());
                callback.response(localResponse, command);
            }

            PartitionUpdate transientResponse = update(row(10, 0, 0), row(10, 3, 3), row(10, 5, 5)).build();
            callback.onResponse(response(command, EP2, iter(transientResponse), false));
            callback.awaitResults();

            List<String> clusterings = new ArrayList<>();
            try (PartitionIterator partitions = resolver.getData())
            {
                while (partitions.hasNext())
                {
                    try (RowIterator rows = partitions.next())
                    {
                        while (rows.hasNext())
                            clusterings.add(AsciiType.instance.getString(rows.next().clustering().bufferAt(0)));
                    }
                }
            }
            Assert.assertEquals(Arrays.asList("3", "4"), clusterings);
        }
        finally
        {
            CoordinatorWarnings.done();
            DatabaseDescriptor.setTombstoneFailureThreshold(originalFailureThreshold);
            cfs.truncateBlocking();
        }
    }

    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas, replicas, null, (self) -> null, Epoch.EMPTY));
    }

    private void waitForLatch(CountDownLatch startlatch)
    {
        try
        {
            startlatch.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
