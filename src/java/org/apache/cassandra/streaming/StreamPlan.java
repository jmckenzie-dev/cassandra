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
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.UUIDGen;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    private final UUID planId = UUIDGen.getTimeUUID();
    private final String description;
    private final List<StreamEventHandler> handlers = new ArrayList<>();
    private int connectionsPerHost = 1;

    // sessions per InetAddress of the other end.
    // private final Map<InetAddress, StreamSession> sessions = new HashMap<>();
    private Map<InetAddress, StreamSessionRoundRobin> sessions = new HashMap<>();
    private final long repairedAt;

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    public StreamPlan(String description)
    {
        this(description, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public StreamPlan(String description, long repairedAt)
    {
        this.description = description;
        this.repairedAt = repairedAt;
    }

    public void setConnectionsPerHost(int value)
    {
        connectionsPerHost = value;
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges)
    {
        return requestRanges(from, keyspace, ranges, new String[0]);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(from);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies), repairedAt);
        return this;
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges)
    {
        return transferRanges(to, keyspace, ranges, new String[0]);
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer, repairedAt);
        return this;
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param sstableDetails sstables with file positions and estimated key count
     * @return this object for chaining
     */
    public StreamPlan transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        // Split up based on # connections per host and distribute evenly
        if (connectionsPerHost > 1)
        {
            ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> lists = populateSubLists(sstableDetails);

            for (ArrayList<StreamSession.SSTableStreamingSections> list : lists)
            {
                StreamSession session = getOrCreateSession(to);
                session.addTransferFiles(list);
            }
        }
        else
        {
            StreamSession session = getOrCreateSession(to);
            session.addTransferFiles(sstableDetails);
        }

        return this;
    }

    private ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> populateSubLists(
            Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        int step = sstableDetails.size() / connectionsPerHost;
        int index = 0;

        int sliceCount = 0;

        ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> result = new ArrayList<>();
        ArrayList<StreamSession.SSTableStreamingSections> slice = null;
        for (StreamSession.SSTableStreamingSections streamSession : sstableDetails)
        {
            System.err.println("Step: " + step + " index: " + index + " slice count: " + sliceCount);
            if (index % step == 0)
            {
                System.err.println("index % step == 0.  Creating new slice.");
                // Add the currently built slice to the result set if we're not at inception
                if (slice != null)
                    result.add(slice);

                slice = new ArrayList<StreamSession.SSTableStreamingSections>();
                ++sliceCount;
            }
            slice.add(streamSession);
            ++index;
        }

        int total = 0;
        for (ArrayList<StreamSession.SSTableStreamingSections> subList : result)
        {
            System.err.println("Count of entries in sublist: " + subList.size());
            total += subList.size();
        }
        System.err.println("Size of input: " + sstableDetails.size() + " and size of total in slices: " + total);
        assert(sstableDetails.size() == total);
        return result;
    }

    public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers)
    {
        this.handlers.add(handler);
        if (handlers != null)
            Collections.addAll(this.handlers, handlers);
        return this;
    }

    /**
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty()
    {
        return sessions.isEmpty();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute()
    {
        Collection<StreamSession> combinedSessions = new ArrayList<StreamSession>();
        for (Map.Entry<InetAddress, StreamSessionRoundRobin> pair : sessions.entrySet())
        {
            for (StreamSession session : pair.getValue())
            {
                combinedSessions.add(session);
            }
        }
        System.err.println("Total combined sessions: " + combinedSessions.size());
        return StreamResultFuture.init(planId, description, combinedSessions, handlers);
    }

    /**
     * Set flushBeforeTransfer option.
     * When it's true, will flush before streaming ranges. (Default: true)
     *
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this object for chaining
     */
    public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer)
    {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }

    private StreamSession getOrCreateSession(InetAddress peer)
    {
        StreamSessionRoundRobin sessionList = sessions.get(peer);
        if (sessionList == null)
        {
            sessionList = new StreamSessionRoundRobin();
            sessions.put(peer, sessionList);
        }

        // Round-robin across all sessions for this host here based on connectionsPerHost
        System.err.println("getting session.");
        return sessionList.getOrCreateNextSession(peer);
    }

    private class StreamSessionRoundRobin implements Iterable<StreamSession>
    {
        private ArrayList<StreamSession> sessions = new ArrayList<StreamSession>();
        private int lastReturned = -1;

        public Iterator<StreamSession> iterator()
        {
            return sessions.iterator();
        }

        public StreamSession getOrCreateNextSession(InetAddress peer)
        {
            // Add a new session if we're under our limit
            if (sessions.size() < connectionsPerHost)
            {
                int newIndex = sessions.size();
                System.err.println("Creating new session with index: " + newIndex);
                sessions.add(new StreamSession(peer, newIndex));
            }

            StreamSession result = sessions.get(++lastReturned);

            if (lastReturned == connectionsPerHost)
                lastReturned = 0;

            System.err.println("Returning session");
            return result;
        }
    }
}
