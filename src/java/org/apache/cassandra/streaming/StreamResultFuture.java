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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A future on the result ({@link StreamState}) of a streaming plan.
 *
 * In practice, this object also groups all the {@link StreamSession} for the streaming job
 * involved. One StreamSession will be created for every peer involved and said session will
 * handle every streaming (outgoing and incoming) to that peer for this job.
 * <p>
 * The future will return a result once every session is completed (successfully or not). If
 * any session ended up with an error, the future will throw a StreamException.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to
 * track progress of the streaming.
 */
public final class StreamResultFuture extends AbstractFuture<StreamState>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamResultFuture.class);

    public final UUID planId;
    public final String description;
    private final Collection<StreamEventHandler> eventListeners = new ConcurrentLinkedQueue<>();

    StreamCoordinator coordinator;
    // private final Map<InetAddress, HostSessions> hostSessions = new NonBlockingHashMap<>();
    // private final Map<InetAddress, ArrayList<StreamSession>> ongoingSessions;
    // private final Map<InetAddress, ArrayList<SessionInfo>> sessionStates = new NonBlockingHashMap<>();

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     */
    private StreamResultFuture(UUID planId, String description, Collection<StreamSession> sessions)
    {
        this.planId = planId;
        this.description = description;
        this.coordinator = new StreamCoordinator(0);

        System.err.println("StreamResultFuture ctor with count of sessions: " + sessions.size());
        for (StreamSession session : sessions)
        {
            System.err.println("StreamResultFuture: Adding a session from the collection in ctor.  Idx: " + session.sessionIndex());
            coordinator.addStreamSession(session);
        }

        // if there is no session to listen to, we immediately set result for returning
        if (sessions.isEmpty())
            set(getCurrentState());
    }

    static StreamResultFuture init(UUID planId, String description, Collection<StreamSession> sessions, Collection<StreamEventHandler> listeners)
    {
        StreamResultFuture future = createAndRegister(planId, description, sessions);
        if (listeners != null)
        {
            for (StreamEventHandler listener : listeners)
                future.addEventListener(listener);
        }

        logger.info("[Stream #{}] Executing streaming plan for {}", planId,  description);

        // start sessions
        for (final StreamSession session : sessions)
        {
            logger.info("[Stream #{}, ID#{}] Beginning stream session with {}", planId, session.sessionIndex(), session.peer);
            session.init(future);
            session.start();
        }

        return future;
    }

    public static synchronized StreamResultFuture initReceivingSide(int sessionIndex,
                                                                    UUID planId,
                                                                    String description,
                                                                    InetAddress from,
                                                                    Socket socket,
                                                                    boolean isForOutgoing,
                                                                    int version) throws IOException
    {
        System.err.println("[initReceivingSide]");
        StreamResultFuture future = StreamManager.instance.getReceivingStream(planId);
        if (future == null)
        {
            System.err.println("   future is null");
            // logger.info("[Stream #{} ID#{}] Creating new streaming plan for {}", planId, sessionIndex, description);
            final StreamSession session = new StreamSession(from, sessionIndex);

            // The main reason we create a StreamResultFuture on the receiving side is for JMX exposure.
            System.err.println("initReceivingSide -> about to create a singleton collection.");
            future = new StreamResultFuture(planId, description, Collections.singleton(session));
            StreamManager.instance.registerReceiving(future);

            session.init(future);
            session.handler.initiateOnReceivingSide(socket, isForOutgoing, version);
        }
        else
        {
            System.err.println("   future is not null.  Attaching socket.");
            future.attachSocket(from, sessionIndex, socket, isForOutgoing, version);
            // logger.info("[Stream #{}, ID#{}] Received streaming plan for {}", planId, sessionIndex, description);
        }
        return future;
    }

    private static StreamResultFuture createAndRegister(UUID planId, String description, Collection<StreamSession> sessions)
    {
        StreamResultFuture future = new StreamResultFuture(planId, description, sessions);
        StreamManager.instance.register(future);
        return future;
    }

    public void attachSocket(InetAddress from, int sessionIndex, Socket socket, boolean isForOutgoing, int version) throws IOException
    {
        StreamSession session = coordinator.getOrCreateSessionById(from, sessionIndex);
        session.init(this);
        session.handler.initiateOnReceivingSide(socket, isForOutgoing, version);
    }

    public void addEventListener(StreamEventHandler listener)
    {
        Futures.addCallback(this, listener);
        eventListeners.add(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    public StreamState getCurrentState()
    {
        return new StreamState(planId, description, coordinator.getAllSessionInfo());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamResultFuture that = (StreamResultFuture) o;
        return planId.equals(that.planId);
    }

    @Override
    public int hashCode()
    {
        return planId.hashCode();
    }

    void handleSessionPrepared(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        logger.info("[Stream #{} ID#{}] Prepare completed. Receiving {} files({} bytes), sending {} files({} bytes)",
                              session.planId(),
                              session.sessionIndex(),
                              sessionInfo.getTotalFilesToReceive(),
                              sessionInfo.getTotalSizeToReceive(),
                              sessionInfo.getTotalFilesToSend(),
                              sessionInfo.getTotalSizeToSend());
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        logger.info("[Stream #{}] Session with {} is complete", session.planId(), session.peer);

        fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        maybeComplete(session);
    }

    public void handleProgress(ProgressInfo progress)
    {
        fireStreamEvent(new StreamEvent.ProgressEvent(planId, progress));
    }

    void fireStreamEvent(StreamEvent event)
    {
        // delegate to listener
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }

    private synchronized void maybeComplete(StreamSession session)
    {
        coordinator.removeStreamSession(session);
        System.err.println("---");
        System.err.println("Checking if maybeComplete");
        System.err.println("---");
        if (!coordinator.hasActiveSessions())
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
            {
                logger.warn("[Stream #{}] Stream failed", planId);
                setException(new StreamException(finalState, "Stream failed"));
            }
            else
            {
                logger.info("[Stream #{}] All sessions completed", planId);
                set(finalState);
            }
        }
    }
}
