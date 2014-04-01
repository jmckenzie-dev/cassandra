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
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link org.apache.cassandra.streaming.StreamCoordinator} is a helper class that abstracts away maintaining multiple
 * StreamSession, SessionInfo, and ProgressInfo instances per peer.
 *
 * This class coordinatea multiple SessionStreams per peer in both the outgoing StreamPlan context and on the
 * inbound StreamResultFuture context.
 */
public class StreamCoordinator
{
    private Map<InetAddress, HostStreamingData> peerSessions = new ConcurrentHashMap<>();
    private int connectionsPerHost = 1;

    public StreamCoordinator(int connectionsPerHost)
    {
        this.connectionsPerHost = connectionsPerHost;
    }

    public boolean hasActiveSessions()
    {
        for (Map.Entry<InetAddress, HostStreamingData> pair : peerSessions.entrySet())
        {
            if (pair.getValue().hasActiveSessions())
                return true;
        }
        return false;
    }

    public void setConnectionsPerHost(int value)
    {
        connectionsPerHost = value;
    }

    public Collection<StreamSession> getAllStreamSessions()
    {
        Collection<StreamSession> results = new ArrayList<>();
        for (Map.Entry<InetAddress, HostStreamingData> pair : peerSessions.entrySet())
        {
            results.addAll(pair.getValue().getAllStreamSessions());
        }
        return results;
    }

    public Set<SessionInfo> getAllSessionInfo()
    {
        Set<SessionInfo> results = new HashSet<>();
        for (Map.Entry<InetAddress, HostStreamingData> pair : peerSessions.entrySet())
        {
            results.addAll(pair.getValue().sessionInfos.values());
        }
        return results;
    }

    public Collection<SessionInfo> getHostSessionInfo(InetAddress peer)
    {
        HostStreamingData data = getHostData(peer);
        return data.sessionInfos.values();
    }

    public Set<InetAddress> getPeers()
    {
        return peerSessions.keySet();
    }

    public ArrayList<ArrayList<StreamSession.SSTableStreamingSections>> sliceSSTableDetails(
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

    public StreamSession getOrCreateNextSession(InetAddress peer)
    {
        HostStreamingData sessionList = getOrCreateHostData(peer);
        return sessionList.getOrCreateNextSession(peer);
    }

    public StreamSession getOrCreateSessionById(InetAddress peer, int id)
    {
        HostStreamingData data = getOrCreateHostData(peer);
        return data.addReceivingStreamSession(peer, id);
    }

    public void addStreamSession(StreamSession session)
    {
        HostStreamingData data = getOrCreateHostData(session.peer);
        data.addStreamSession(session);
    }

    public void removeStreamSession(StreamSession session)
    {
        HostStreamingData data = getHostData(session.peer);
        data.removeStreamSession(session);
    }

    public void addSessionInfo(SessionInfo info)
    {
        System.err.println("SC: addSessionInfo: " + info.peer + " id: " + info.sessionIndex);
        HostStreamingData data = getOrCreateHostData(info.peer);
        data.sessionInfos.put(info.sessionIndex, info);
    }

    public void updateProgress(ProgressInfo info)
    {
        HostStreamingData data = getHostData(info.peer);
        data.updateProgress(info);
    }

    public Collection<ProgressInfo> getSessionProgress(InetAddress peer, int index)
    {
        HostStreamingData data = getHostData(peer);
        return data.getSessionProgress(index);
    }

    public void transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
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

            for (ArrayList<StreamSession.SSTableStreamingSections> list : lists)
            {
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
        private Map<Integer, StreamSession> streamSessions = new ConcurrentHashMap<>();
        private Map<Integer, Map<String, ProgressInfo>> progressInfos = new ConcurrentHashMap<>();

        public Map<Integer, SessionInfo> sessionInfos = new ConcurrentHashMap<>();
        public int lastReturned = -1;

        public boolean hasActiveSessions()
        {
            return streamSessions.size() > 0;
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

        public StreamSession addReceivingStreamSession(InetAddress from, int id)
        {
            StreamSession session = new StreamSession(from, id);
            streamSessions.put(id, session);
            return session;
        }

        public void removeStreamSession(StreamSession session)
        {
            streamSessions.remove(session.sessionIndex());
        }

        public Collection<StreamSession> getAllStreamSessions()
        {
            return streamSessions.values();
        }

        public void updateProgress(ProgressInfo info)
        {
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
            return progressInfos.get(index).values();
        }
    }
}
