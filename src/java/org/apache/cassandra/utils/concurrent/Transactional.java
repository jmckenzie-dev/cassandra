/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * An abstraction for Transactional behaviour. An object implementing this interface has a lifetime
 * of the following pattern:
 *
 * try (Transactional t1, t2 = ...)
 * {
 *     // do work with t1 and t2
 *     t1.prepareForCommit();
 *     t2.prepareForCommit();
 *     t1.commit();
 *     t2.commit();
 * }
 *
 * If something goes wrong before commit() is called on any transaction, then on exiting the try block
 * the auto close method will invoke abort() and cleanup() to reset any state.
 *
 * If everything completes normally, then on exiting the try block the auto close method will invoke cleanup
 * to release any temporary state/resources
 *
 * prepareForCommit() is left undefined in this interface, as it is implementation dependent; only the commit
 * state and cleanup/abort logic are defined here, and some utility classes provided to make it easy to use the
 * abstraction.
 *
 * All of the methods defined here accept and return a Throwable, and should attempt to ensure no exception can
 * be thrown during their execution unless throwing it is likely to leave the program in a safer state (such
 * as attempting to commit a non-prepared transactional).
 *
 * In general, implementations should utilise a StateManager object internally for controlling behaviour inside
 * of each method, asking permission to perform work via the beginTransition() method, and invoking rejectedTransition()
 * for the result if this indicates the state does not permit the action. Once the action is completed completeTransition()
 * is called. The autoclose() method is then invoked from the implementor's close(), which ensures commit() and rollback()
 * are each dealt with correctly.
 */
public interface Transactional extends AutoCloseable
{

    // A simple inner class for managing state transitions
    public static class StateManager
    {
        private State state = State.IN_PROGRESS;
        private final Transactional transactional;

        public StateManager(Transactional transactional)
        {
            this.transactional = transactional;
        }

        public void autoclose()
        {
            Throwable fail = null;
            switch (state)
            {
                case COMMITTED:
                    state = State.COMMITTED;
                    fail = transactional.cleanup(fail);
                    break;
                case ABORTED:
                    state = State.ABORTED;
                    break;
                default:
                    state = State.ABORTED;
                    fail = transactional.abort(fail);
                    fail = transactional.cleanup(fail);
            }
            maybeFail(fail);
        }

        public Throwable noOpTransition(State newState, Throwable accumulate)
        {
            if (!beginTransition(newState))
                return rejectedTransition(newState, accumulate);
            completeTransition(newState);
            return accumulate;
        }

        public boolean beginTransition(State newState)
        {
            if (state == newState)
                return false;

            if (newState.validPrecedents.contains(state))
            {
                // set aborted status immediately, as if we fail in midst we should not retry
                if (newState == State.ABORTED)
                    state = newState;
                return true;
            }

            // we fail fast here, because it's a complete misuse of the API, and it's not clear what the behaviour should be
            // and it's probably better to have a partial rollback
            if (newState == State.COMMITTED)
                throw new IllegalStateException("Cannot transition from " + state + " to " + newState);

            return false;
        }

        public Throwable rejectedTransition(State newState, Throwable accumulate)
        {
            if (state == newState)
            {
                if (state == State.READY_TO_COMMIT)
                    throw new IllegalStateException("Preparing to commit multiple times may be dangerous, and is forbidden");
                return accumulate;
            }

            try
            {
                throw new IllegalStateException("Cannot transition from " + state + " to " + newState);
            }
            catch (Throwable t)
            {
                return merge(accumulate, t);
            }
        }

        public void completeTransition(State newState)
        {
            state = newState;
        }

        public State get()
        {
            return state;
        }
    }

    public static enum State
    {
        IN_PROGRESS(),
        READY_TO_COMMIT(IN_PROGRESS),
        COMMITTED(READY_TO_COMMIT),
        ABORTED(IN_PROGRESS, READY_TO_COMMIT);

        final Set<State> validPrecedents;

        State(State ... validPrecedents)
        {
            this.validPrecedents = ImmutableSet.copyOf(validPrecedents);
        }
    }

    // THERE SHOULD BE A METHOD NAMED prepareToCommit() THAT DOES ALL WORK
    // THAT MAY THROW AN EXCEPTION ON COMMIT, AND TRANSITIONS THE STATE TO READY_TO_COMMIT
    // THE PARAMETERS TO THIS METHOD ARE ARBITRARY, SO IT IS NOT DEFINED HERE

    // commit should never throw an exception, and preferably never generate one,
    // but if it does generate one it should accumulate it in the parameter and return the result
    public Throwable commit(Throwable failed);

    // rollback all changes
    public Throwable abort(Throwable failed);

    // cleanup once done (shared cleanup regardless of if committed or aborted)
    public Throwable cleanup(Throwable failed);

}
