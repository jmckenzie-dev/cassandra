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

/**
 * {@link org.apache.cassandra.streaming.StreamCoordinator} is a helper class that abstracts away maintaining multiple
 * StreamSession and ProgressInfo instances per peer.
 *
 * This class coordinatea multiple SessionStreams per peer in both the outgoing StreamPlan context and on the
 * inbound StreamResultFuture context.
 */
public class StreamCoordinator
{
    private Map<InetAddress, HostStreamingData> peerSessions = new HashMap<>();
    private int connectionsPerHost = 1;

    public StreamCoordinator(int connectionsPerHost)
    {
        this.connectionsPerHost = connectionsPerHost;
    }

    public synchronized boolean hasActiveSessions()
    {
        for (Map.Entry<InetAddress, HostStreamingData> pair : peerSessions.entrySet())
        {
            if (pair.getValue().hasActiveSessions())
                return true;
        }
        return false;
    }

    public synchronized void setConnectionsPerHost(int value)
    {
        connectionsPerHost = value;
    }

    public synchronized Collection<StreamSession> getAllStreamSessions()
    {
        Collection<StreamSession> results = new ArrayList<>();
        for (Map.Entry<InetAddress, HostStreamingData> pair : peerSessions.entrySet())
        {
            results.addAll(pair.getValue().getAllStreamSessions());
        }
        return results;
    }

    public synchronized Set<InetAddress> getPeers()
    {
        return new HashSet<>(peerSessions.keySet());
    }

    public synchronized StreamSession getOrCreateNextSession(InetAddress peer)
    {
        return getOrCreateHostData(peer).getOrCreateNextSession(peer);
    }

    public synchronized StreamSession getOrCreateSessionById(InetAddress peer, int id)
    {
        return getOrCreateHostData(peer).getOrCreateSessionById(peer, id);
    }

    public synchronized void addStreamSession(StreamSession session)
    {
        getOrCreateHostData(session.peer).addStreamSession(session);
    }

    public synchronized void updateProgress(ProgressInfo info)
    {
        getHostData(info.peer).updateProgress(info);
    }

    public synchronized Collection<ProgressInfo> getSessionProgress(InetAddress peer, int index)
    {
        return getHostData(peer).getSessionProgress(index);
    }

    public synchronized void addSessionInfo(SessionInfo session)
    {
        HostStreamingData data = getOrCreateHostData(session.peer);
        data.addSessionInfo(session);
    }

    public synchronized Collection<SessionInfo> getHostSessionInfo(InetAddress peer)
    {
        return getHostData(peer).getAllSessionInfo();
    }

    public synchronized Set<SessionInfo> getAllSessionInfo()
    {
        Set<SessionInfo> result = new HashSet<>();
        for (HostStreamingData data : peerSessions.values())
        {
            result.addAll(data.getAllSessionInfo());
        }
        return result;
    }

    public synchronized void transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        HostStreamingData sessionList = peerSessions.get(to);
        if (sessionList == null)
        {
            sessionList = new HostStreamingData();
            peerSessions.put(to, sessionList);
        }

        if (connectionsPerHost > 1)
        {
            ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> lists = sliceSSTableDetails(sstableDetails);

            int idx = 0;
            for (ArrayList<StreamSession.SSTableStreamingSections> list : lists)
            {
                System.err.println("Adding SSTableStreamingSections to idx: " + idx + " with files:");
                for (StreamSession.SSTableStreamingSections section : list)
                {
                    System.err.println("   " + section.sstable.getColumnFamilyName());
                }

                StreamSession session = sessionList.getOrCreateNextSession(to);
                session.addTransferFiles(list);
            }
        }
        else
        {
            StreamSession session = sessionList.getOrCreateNextSession(to);
            session.addTransferFiles(sstableDetails);
        }
    }

    private ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> sliceSSTableDetails(
        Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        int step = Math.round((float)sstableDetails.size() / (float)connectionsPerHost);
        int index = 0;

        ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> result = new ArrayList<>();
        ArrayList<StreamSession.SSTableStreamingSections> slice = null;
        for (StreamSession.SSTableStreamingSections streamSession : sstableDetails)
        {
            if (index % step == 0)
            {
                slice = new ArrayList<>();
                result.add(slice);
            }
            slice.add(streamSession);
            ++index;
        }

        return result;
    }

    private HostStreamingData getHostData(InetAddress peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
            throw new IllegalArgumentException("Unknown peer requested: " + peer.toString());
        return data;
    }

    private HostStreamingData getOrCreateHostData(InetAddress peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
        {
            data = new HostStreamingData();
            peerSessions.put(peer, data);
        }
        return data;
    }

    private class HostStreamingData
    {
        private Map<Integer, StreamSession> streamSessions = new HashMap<>();
        private Map<Integer, SessionInfo> sessionInfos = new HashMap<>();
        private Map<Integer, Map<String, ProgressInfo>> progressInfos = null;

        public int lastReturned = -1;

        public boolean hasActiveSessions()
        {
            for (Map.Entry<Integer, StreamSession> pair : streamSessions.entrySet())
            {
                if (!pair.getValue().isComplete())
                    return true;
            }
            return false;
        }

        public StreamSession getOrCreateNextSession(InetAddress peer)
        {
            // create
            if (streamSessions.size() < connectionsPerHost)
            {
                StreamSession session = new StreamSession(peer, streamSessions.size());
                streamSessions.put(++lastReturned, session);
                return session;
            }
            // get
            else
            {
                if (lastReturned == streamSessions.size() - 1)
                    lastReturned = 0;

                return streamSessions.get(lastReturned++);
            }
        }

        public void addStreamSession(StreamSession session)
        {
            streamSessions.put(streamSessions.size(), session);
        }

        public StreamSession getOrCreateSessionById(InetAddress peer, int id)
        {
            StreamSession session = streamSessions.get(id);
            if (session == null)
            {
                session = new StreamSession(peer, id);
                streamSessions.put(id, session);
            }
            return session;
        }

        public Collection<StreamSession> getAllStreamSessions()
        {
            return new ArrayList<>(streamSessions.values());
        }

        public void updateProgress(ProgressInfo info)
        {
            // lazy init -> ProgressInfo not used on both sides of Stream
            if (progressInfos == null)
                progressInfos = new HashMap<>();

            Map<String, ProgressInfo> progresses = progressInfos.get(info.sessionIndex);
            if (progresses == null)
            {
                progresses = new HashMap<>();
                progressInfos.put(info.sessionIndex, progresses);
            }
            progresses.put(info.fileName, info);
        }

        public Collection<ProgressInfo> getSessionProgress(int index)
        {
            // lazy init -> ProgressInfo not used on both sides of Stream
            if (progressInfos == null)
                progressInfos = new HashMap<>();

            Map<String, ProgressInfo> progresses = progressInfos.get(index);
            if (progresses == null)
                return new ArrayList<>();

            // return copy to prevent ConcurrentModificationException while iterating
            return new ArrayList<>(progressInfos.get(index).values());
        }

        public void addSessionInfo(SessionInfo info)
        {
            System.err.println("State of map prior to add:");
            for (Map.Entry<Integer, SessionInfo> pair : sessionInfos.entrySet())
            {
                System.err.println("SI for index: " + pair.getValue().sessionIndex);
            }
            if (info == null)
            {
                System.err.println("ADDING NULL BAD.");
                throw new IllegalArgumentException("BAD NULL");
            }

            sessionInfos.put(info.sessionIndex, info);
            System.err.println("State of map after add:");
            for (Map.Entry<Integer, SessionInfo> pair : sessionInfos.entrySet())
            {
                System.err.println("SI for index: " + pair.getValue().sessionIndex);
            }
        }

        public Collection<SessionInfo> getAllSessionInfo()
        {
            System.err.println("size of sessionInfo in getAllSessionInfo: " + sessionInfos.size());
            return new ArrayList<>(sessionInfos.values());
        }
    }
}
