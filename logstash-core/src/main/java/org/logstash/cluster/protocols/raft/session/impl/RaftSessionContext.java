package org.logstash.cluster.protocols.raft.session.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.phi.PhiAccrualFailureDetector;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.impl.OperationResult;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.protocols.raft.protocol.PublishRequest;
import org.logstash.cluster.protocols.raft.protocol.RaftServerProtocol;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.service.impl.DefaultServiceContext;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.session.RaftSessionEvent;
import org.logstash.cluster.protocols.raft.session.RaftSessionEventListener;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.utils.TimestampPrinter;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.logstash.cluster.utils.concurrent.ThreadContextFactory;

/**
 * Raft session.
 */
public class RaftSessionContext implements RaftSession {

    private static final Logger LOGGER = LogManager.getLogger(RaftSessionContext.class);

    private final SessionId sessionId;
    private final MemberId member;
    private final String name;
    private final ServiceType serviceType;
    private final ReadConsistency readConsistency;
    private final long minTimeout;
    private final long maxTimeout;
    private final RaftServerProtocol protocol;
    private final DefaultServiceContext context;
    private final RaftContext server;
    private final ThreadContext eventExecutor;
    private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
    private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();
    private final Map<Long, OperationResult> results = new HashMap<>();
    private final Queue<EventHolder> events = new LinkedList<>();
    private final Set<RaftSessionEventListener> eventListeners = new CopyOnWriteArraySet<>();
    private volatile State state = State.OPEN;
    private volatile long lastUpdated;
    private long lastHeartbeat;
    private PhiAccrualFailureDetector failureDetector = new PhiAccrualFailureDetector();
    private long requestSequence;
    private volatile long commandSequence;
    private volatile long lastApplied;
    private volatile long commandLowWaterMark;
    private volatile long eventIndex;
    private volatile long completeIndex;
    private volatile EventHolder currentEventList;

    public RaftSessionContext(
        SessionId sessionId,
        MemberId member,
        String name,
        ServiceType serviceType,
        ReadConsistency readConsistency,
        long minTimeout,
        long maxTimeout,
        DefaultServiceContext context,
        RaftContext server,
        ThreadContextFactory threadContextFactory) {
        this.sessionId = sessionId;
        this.member = member;
        this.name = name;
        this.serviceType = serviceType;
        this.readConsistency = readConsistency;
        this.minTimeout = minTimeout;
        this.maxTimeout = maxTimeout;
        this.eventIndex = sessionId.id();
        this.completeIndex = sessionId.id();
        this.lastApplied = sessionId.id();
        this.protocol = server.getProtocol();
        this.context = context;
        this.server = server;
        this.eventExecutor = threadContextFactory.createContext();
        protocol.registerResetListener(sessionId, request -> resendEvents(request.index()), context.executor());
    }

    /**
     * Resends events from the given sequence.
     * @param index The index from which to resend events.
     */
    public void resendEvents(long index) {
        clearEvents(index);
        for (EventHolder event : events) {
            sendEvents(event);
        }
    }

    /**
     * Clears events up to the given sequence.
     * @param index The index to clear.
     */
    private void clearEvents(long index) {
        if (index > completeIndex) {
            EventHolder event = events.peek();
            while (event != null && event.eventIndex <= index) {
                events.remove();
                completeIndex = event.eventIndex;
                event = events.peek();
            }
            completeIndex = index;
        }
    }

    /**
     * Sends an event to the session.
     */
    private void sendEvents(EventHolder event) {
        // Only send events to the client if this server is the leader.
        if (server.isLeader()) {
            eventExecutor.execute(() -> {
                PublishRequest request = PublishRequest.builder()
                    .withSession(sessionId().id())
                    .withEventIndex(event.eventIndex)
                    .withPreviousIndex(Math.max(event.previousIndex, completeIndex))
                    .withEvents(event.events)
                    .build();

                LOGGER.trace("Sending {}", request);
                protocol.publish(member, request);
            });
        }
    }

    @Override
    public SessionId sessionId() {
        return sessionId;
    }

    @Override
    public String serviceName() {
        return name;
    }

    @Override
    public ServiceType serviceType() {
        return serviceType;
    }

    @Override
    public MemberId memberId() {
        return member;
    }

    @Override
    public ReadConsistency readConsistency() {
        return readConsistency;
    }

    @Override
    public long maxTimeout() {
        return maxTimeout;
    }

    @Override
    public long minTimeout() {
        return minTimeout;
    }

    @Override
    public State getState() {
        return state;
    }

    /**
     * Updates the session state.
     * @param state The session state.
     */
    private void setState(State state) {
        if (this.state != state) {
            this.state = state;
            LOGGER.debug("State changed: {}", state);
            switch (state) {
                case OPEN:
                    eventListeners.forEach(l -> l.onEvent(new RaftSessionEvent(RaftSessionEvent.Type.OPEN, this, getLastUpdated())));
                    break;
                case EXPIRED:
                    eventListeners.forEach(l -> l.onEvent(new RaftSessionEvent(RaftSessionEvent.Type.EXPIRE, this, getLastUpdated())));
                    break;
                case CLOSED:
                    eventListeners.forEach(l -> l.onEvent(new RaftSessionEvent(RaftSessionEvent.Type.CLOSE, this, getLastUpdated())));
                    break;
            }
        }
    }

    /**
     * Returns the session update timestamp.
     * @return The session update timestamp.
     */
    public long getLastUpdated() {
        return lastUpdated;
    }

    /**
     * Updates the session timestamp.
     * @param lastUpdated The session timestamp.
     */
    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = Math.max(this.lastUpdated, lastUpdated);
    }

    @Override
    public void addListener(RaftSessionEventListener listener) {
        eventListeners.add(listener);
    }

    @Override
    public void removeListener(RaftSessionEventListener listener) {
        eventListeners.remove(listener);
    }

    @Override
    public void publish(RaftEvent event) {
        // Store volatile state in a local variable.
        State state = this.state;
        Preconditions.checkState(state != State.EXPIRED, "session is expired");
        Preconditions.checkState(state != State.CLOSED, "session is closed");
        Preconditions.checkState(context.currentOperation() == OperationType.COMMAND, "session events can only be published during command execution");

        // If the client acked an index greater than the current event sequence number since we know the
        // client must have received it from another server.
        if (completeIndex > context.currentIndex()) {
            return;
        }

        // If no event has been published for this index yet, create a new event holder.
        if (this.currentEventList == null || this.currentEventList.eventIndex != context.currentIndex()) {
            long previousIndex = eventIndex;
            eventIndex = context.currentIndex();
            this.currentEventList = new EventHolder(eventIndex, previousIndex);
        }

        // Add the event to the event holder.
        this.currentEventList.events.add(event);
    }

    /**
     * Returns the state machine context associated with the session.
     * @return The state machine context associated with the session.
     */
    public DefaultServiceContext getService() {
        return context;
    }

    /**
     * Returns a boolean indicating whether the session is timed out.
     * @param timestamp the current timestamp
     * @return indicates whether the session is timed out
     */
    public boolean isTimedOut(long timestamp) {
        long lastUpdated = this.lastUpdated;
        return lastUpdated > 0 && timestamp - lastUpdated > maxTimeout;
    }

    /**
     * Returns the current heartbeat time.
     * @return The current heartbeat time.
     */
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    /**
     * Sets the last heartbeat time.
     * @param lastHeartbeat The last heartbeat time.
     */
    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = Math.max(this.lastHeartbeat, lastHeartbeat);
        failureDetector.report(lastHeartbeat);
    }

    /**
     * Resets heartbeat times.
     */
    public void resetHeartbeats() {
        this.lastHeartbeat = 0;
        this.failureDetector = new PhiAccrualFailureDetector();
    }

    /**
     * Returns a boolean indicating whether the session appears to have failed due to lack of heartbeats.
     * @param threshold The phi failure threshold
     * @return Indicates whether the session has failed.
     */
    public boolean isFailed(int threshold) {
        return failureDetector.phi() >= threshold;
    }

    /**
     * Returns the session request number.
     * @return The session request number.
     */
    public long getRequestSequence() {
        return requestSequence;
    }

    /**
     * Sets the current request sequence number.
     * @param requestSequence the current request sequence number
     */
    public void setRequestSequence(long requestSequence) {
        this.requestSequence = Math.max(this.requestSequence, requestSequence);
    }

    /**
     * Returns the next request sequence number.
     * @return the next request sequence number
     */
    public long nextRequestSequence() {
        return this.requestSequence + 1;
    }

    /**
     * Resets the current request sequence number.
     * @param requestSequence The request sequence number.
     */
    public void resetRequestSequence(long requestSequence) {
        // If the request sequence number is less than the applied sequence number, update the request
        // sequence number. This is necessary to ensure that if the local server is a follower that is
        // later elected leader, its sequences are consistent for commands.
        if (requestSequence > this.requestSequence) {
            this.requestSequence = requestSequence;
        }
    }

    /**
     * Returns the session operation sequence number.
     * @return The session operation sequence number.
     */
    public long getCommandSequence() {
        return commandSequence;
    }

    /**
     * Sets the session operation sequence number.
     * @param sequence The session operation sequence number.
     */
    public void setCommandSequence(long sequence) {
        // For each increment of the sequence number, trigger query callbacks that are dependent on the specific sequence.
        for (long i = commandSequence + 1; i <= sequence; i++) {
            commandSequence = i;
            List<Runnable> queries = this.sequenceQueries.remove(commandSequence);
            if (queries != null) {
                for (Runnable query : queries) {
                    query.run();
                }
            }
        }
    }

    /**
     * Returns the next operation sequence number.
     * @return The next operation sequence number.
     */
    public long nextCommandSequence() {
        return commandSequence + 1;
    }

    /**
     * Returns the session index.
     * @return The session index.
     */
    public long getLastApplied() {
        return lastApplied;
    }

    /**
     * Sets the session index.
     * @param index The session index.
     */
    public void setLastApplied(long index) {
        // Query callbacks for this session are added to the indexQueries map to be executed once the required index
        // for the query is reached. For each increment of the index, trigger query callbacks that are dependent
        // on the specific index.
        for (long i = lastApplied + 1; i <= index; i++) {
            lastApplied = i;
            List<Runnable> queries = this.indexQueries.remove(lastApplied);
            if (queries != null) {
                for (Runnable query : queries) {
                    query.run();
                }
            }
        }
    }

    /**
     * Registers a causal session query.
     * @param sequence The session sequence number at which to execute the query.
     * @param query The query to execute.
     */
    public void registerSequenceQuery(long sequence, Runnable query) {
        // Add a query to be run once the session's sequence number reaches the given sequence number.
        List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> new LinkedList<>());
        queries.add(query);
    }

    /**
     * Registers a session index query.
     * @param index The state machine index at which to execute the query.
     * @param query The query to execute.
     */
    public void registerIndexQuery(long index, Runnable query) {
        // Add a query to be run once the session's index reaches the given index.
        List<Runnable> queries = this.indexQueries.computeIfAbsent(index, v -> new LinkedList<>());
        queries.add(query);
    }

    /**
     * Registers a session result.
     * <p>
     * Results are stored in memory on all servers in order to provide linearizable semantics. When a command
     * is applied to the state machine, the command's return value is stored with the sequence number. Once the
     * client acknowledges receipt of the command output the result will be cleared from memory.
     * @param sequence The result sequence number.
     * @param result The result.
     */
    public void registerResult(long sequence, OperationResult result) {
        results.put(sequence, result);
    }

    /**
     * Clears command results up to the given sequence number.
     * <p>
     * Command output is removed from memory up to the given sequence number. Additionally, since we know the
     * client received a response for all commands up to the given sequence number, command futures are removed
     * from memory as well.
     * @param sequence The sequence to clear.
     */
    public void clearResults(long sequence) {
        if (sequence > commandLowWaterMark) {
            for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
                results.remove(i);
                commandLowWaterMark = i;
            }
        }
    }

    /**
     * Returns the session response for the given sequence number.
     * @param sequence The response sequence.
     * @return The response.
     */
    public OperationResult getResult(long sequence) {
        return results.get(sequence);
    }

    /**
     * Returns the session event index.
     * @return The session event index.
     */
    public long getEventIndex() {
        return eventIndex;
    }

    /**
     * Sets the session event index.
     * @param eventIndex the session event index
     */
    public void setEventIndex(long eventIndex) {
        this.eventIndex = eventIndex;
    }

    /**
     * Commits events for the given index.
     */
    public void commit(long index) {
        if (currentEventList != null && currentEventList.eventIndex == index) {
            events.add(currentEventList);
            sendEvents(currentEventList);
        }
        setLastApplied(index);
    }

    /**
     * Returns the index of the highest event acked for the session.
     * @return The index of the highest event acked for the session.
     */
    public long getLastCompleted() {
        // If there are any queued events, return the index prior to the first event in the queue.
        EventHolder event = events.peek();
        if (event != null && event.eventIndex > completeIndex) {
            return event.eventIndex - 1;
        }
        // If no events are queued, return the highest index applied to the session.
        return lastApplied;
    }

    /**
     * Sets the last completed event index for the session.
     * @param lastCompleted the last completed index
     */
    public void setLastCompleted(long lastCompleted) {
        this.completeIndex = lastCompleted;
    }

    /**
     * Expires the session.
     */
    public void expire() {
        setState(State.EXPIRED);
        protocol.unregisterResetListener(sessionId);
    }

    /**
     * Closes the session.
     */
    public void close() {
        setState(State.CLOSED);
        protocol.unregisterResetListener(sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), sessionId);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof RaftSession && ((RaftSession) object).sessionId() == sessionId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .addValue(context)
            .add("session", sessionId)
            .add("timestamp", TimestampPrinter.of(lastUpdated))
            .toString();
    }

    /**
     * Event holder.
     */
    private static class EventHolder {
        private final long eventIndex;
        private final long previousIndex;
        private final List<RaftEvent> events = new LinkedList<>();

        private EventHolder(long eventIndex, long previousIndex) {
            this.eventIndex = eventIndex;
            this.previousIndex = previousIndex;
        }
    }

}