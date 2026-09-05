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
package org.apache.cassandra.db.compaction;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.memory.HeapCloner;

final class TombstoneTriggeredCompactionManager
{
    private static final Logger logger = LoggerFactory.getLogger(TombstoneTriggeredCompactionManager.class);

    enum AdmissionResult
    {
        ACCEPTED,
        DUPLICATE,
        FULL,
        DISABLED,
        SHUTDOWN
    }

    enum ExecutionResult
    {
        COMPLETED,
        BUSY
    }

    interface TaskRunner
    {
        ExecutionResult run(TableId tableId, DecoratedKey key) throws Exception;
    }

    private final IntSupplier capacity;
    private final TaskRunner taskRunner;
    private final ExecutorService executor;
    private final long retryDelayMillis;
    private final Set<Request> pending = new LinkedHashSet<>();

    private Request active;
    private boolean draining;
    private boolean shutdown;
    private boolean forceShutdown;

    TombstoneTriggeredCompactionManager(IntSupplier capacity,
                                        TaskRunner taskRunner,
                                        ExecutorService executor,
                                        long retryDelayMillis)
    {
        this.capacity = capacity;
        this.taskRunner = taskRunner;
        this.executor = executor;
        this.retryDelayMillis = retryDelayMillis;
    }

    AdmissionResult enqueue(TableId tableId, DecoratedKey key)
    {
        synchronized (this)
        {
            if (shutdown)
                return AdmissionResult.SHUTDOWN;

            int currentCapacity = capacity.getAsInt();
            if (currentCapacity == 0)
                return AdmissionResult.DISABLED;
            Request request = new Request(tableId, key);
            if (request.equals(active) || pending.contains(request))
                return AdmissionResult.DUPLICATE;
            if (pending.size() + (active == null ? 0 : 1) >= currentCapacity)
                return AdmissionResult.FULL;

            pending.add(new Request(tableId, HeapCloner.instance.clone(key)));
            if (!draining)
            {
                draining = true;
                try
                {
                    executor.execute(this::drain);
                }
                catch (RejectedExecutionException e)
                {
                    draining = false;
                    pending.remove(request);
                    shutdown = true;
                    return AdmissionResult.SHUTDOWN;
                }
            }
            return AdmissionResult.ACCEPTED;
        }
    }

    synchronized int outstandingTasks()
    {
        return pending.size() + (active == null ? 0 : 1);
    }

    synchronized boolean hasTasks()
    {
        return draining || active != null || !pending.isEmpty();
    }

    void shutdown(boolean force)
    {
        synchronized (this)
        {
            shutdown = true;
            forceShutdown = force;
            if (force)
            {
                pending.clear();
                if (active == null)
                    draining = false;
            }
        }

        if (force)
            executor.shutdownNow();
        else
            executor.shutdown();
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return executor.awaitTermination(timeout, unit);
    }

    private void drain()
    {
        int consecutiveBusy = 0;
        while (true)
        {
            Request request;
            synchronized (this)
            {
                if (forceShutdown || pending.isEmpty())
                {
                    active = null;
                    draining = false;
                    return;
                }

                request = pending.iterator().next();
                pending.remove(request);
                active = request;
            }

            try
            {
                if (taskRunner.run(request.tableId, request.key) == ExecutionResult.BUSY)
                {
                    int pendingCount;
                    synchronized (this)
                    {
                        active = null;
                        if (!shutdown)
                            pending.add(request);
                        pendingCount = pending.size();
                    }

                    consecutiveBusy++;
                    if (pendingCount > 0 && consecutiveBusy >= pendingCount)
                    {
                        Thread.sleep(retryDelayMillis);
                        consecutiveBusy = 0;
                    }
                }
                else
                {
                    synchronized (this)
                    {
                        active = null;
                    }
                    consecutiveBusy = 0;
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                synchronized (this)
                {
                    active = null;
                    draining = false;
                }
                return;
            }
            catch (Exception e)
            {
                logger.error("Tombstone-triggered compaction failed for table {} at token {}",
                             request.tableId, request.key.getToken(), e);
                synchronized (this)
                {
                    active = null;
                }
                consecutiveBusy = 0;
            }
        }
    }

    private static final class Request
    {
        private final TableId tableId;
        private final DecoratedKey key;

        private Request(TableId tableId, DecoratedKey key)
        {
            this.tableId = tableId;
            this.key = key;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (!(obj instanceof Request))
                return false;
            Request that = (Request) obj;
            return tableId.equals(that.tableId) && key.equals(that.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, key);
        }
    }
}
