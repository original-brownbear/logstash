package org.logstash.cluster.protocols.raft.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.service.impl.DefaultRaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.session.RaftSessions;
import org.logstash.cluster.time.Clock;
import org.logstash.cluster.time.LogicalClock;
import org.logstash.cluster.time.WallClock;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.Scheduler;

/**
 * Raft service.
 */
public abstract class AbstractRaftService implements RaftService {
    private static final Logger LOGGER = LogManager.getLogger(AbstractRaftService.class);
    private ServiceContext context;
    private RaftServiceExecutor executor;

    @Override
    public void init(ServiceContext context) {
        this.context = context;
        this.executor = new DefaultRaftServiceExecutor(context);
        configure(executor);
    }

    @Override
    public void tick(WallClockTimestamp timestamp) {
        executor.tick(timestamp);
    }

    @Override
    public byte[] apply(Commit<byte[]> commit) {
        return executor.apply(commit);
    }

    /**
     * Configures the state machine.
     * <p>
     * By default, this method will configure state machine operations by extracting public methods with
     * a single {@link Commit} parameter via reflection. Override this method to explicitly register
     * state machine operations via the provided {@link RaftServiceExecutor}.
     * @param executor The state machine executor.
     */
    protected abstract void configure(RaftServiceExecutor executor);

    /**
     * Returns the service context.
     * @return the service context
     */
    protected ServiceContext context() {
        return context;
    }

    /**
     * Returns the service logger.
     * @return the service logger
     */
    protected Logger logger() {
        return LOGGER;
    }

    /**
     * Returns the state machine scheduler.
     * @return The state machine scheduler.
     */
    protected Scheduler scheduler() {
        return executor;
    }

    /**
     * Returns the unique state machine identifier.
     * @return The unique state machine identifier.
     */
    protected ServiceId serviceId() {
        return context.serviceId();
    }

    /**
     * Returns the unique state machine name.
     * @return The unique state machine name.
     */
    protected String serviceName() {
        return context.serviceName();
    }

    /**
     * Returns the state machine's current index.
     * @return The state machine's current index.
     */
    protected long currentIndex() {
        return context.currentIndex();
    }

    /**
     * Returns the state machine's clock.
     * @return The state machine's clock.
     */
    protected Clock clock() {
        return wallClock();
    }

    /**
     * Returns the state machine's wall clock.
     * @return The state machine's wall clock.
     */
    protected WallClock wallClock() {
        return context.wallClock();
    }

    /**
     * Returns the state machine's logical clock.
     * @return The state machine's logical clock.
     */
    protected LogicalClock logicalClock() {
        return context.logicalClock();
    }

    /**
     * Returns the sessions registered with the state machines.
     * @return The state machine's sessions.
     */
    protected RaftSessions sessions() {
        return context.sessions();
    }

    @Override
    public void onOpen(RaftSession session) {

    }

    @Override
    public void onExpire(RaftSession session) {

    }

    @Override
    public void onClose(RaftSession session) {

    }
}
