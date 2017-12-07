package org.logstash.cluster.primitives.leadership.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.leadership.Leader;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.primitives.leadership.LeadershipEvent;
import org.logstash.cluster.primitives.leadership.LeadershipEvent.Type;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.Anoint;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.Evict;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.Promote;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.Run;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.Withdraw;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * State machine for {@link RaftLeaderElector} resource.
 */
public class RaftLeaderElectorService extends AbstractRaftService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(RaftLeaderElectorOperations.NAMESPACE)
        .register(RaftLeaderElectorEvents.NAMESPACE)
        .register(Registration.class)
        .register(new LinkedHashMap<>().keySet().getClass())
        .build());

    private Registration leader;
    private long term;
    private long termStartTime;
    private List<Registration> registrations = new LinkedList<>();
    private AtomicLong termCounter = new AtomicLong();
    private Map<Long, RaftSession> listeners = new LinkedHashMap<>();

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(termCounter.get());
        writer.writeObject(leader, SERIALIZER::encode);
        writer.writeLong(term);
        writer.writeLong(termStartTime);
        writer.writeObject(registrations, SERIALIZER::encode);
        writer.writeObject(Sets.newHashSet(listeners.keySet()), SERIALIZER::encode);
        logger().debug("Took state machine snapshot");
    }

    @Override
    public void install(SnapshotReader reader) {
        termCounter.set(reader.readLong());
        leader = reader.readObject(SERIALIZER::decode);
        term = reader.readLong();
        termStartTime = reader.readLong();
        registrations = reader.readObject(SERIALIZER::decode);
        listeners = new LinkedHashMap<>();
        for (Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
            listeners.put(sessionId, sessions().getSession(sessionId));
        }
        logger().debug("Reinstated state machine from snapshot");
    }

    @Override
    protected void configure(RaftServiceExecutor executor) {
        // Notification
        executor.register(RaftLeaderElectorOperations.ADD_LISTENER, this::listen);
        executor.register(RaftLeaderElectorOperations.REMOVE_LISTENER, this::unlisten);
        // Commands
        executor.register(RaftLeaderElectorOperations.RUN, SERIALIZER::decode, this::run, SERIALIZER::encode);
        executor.register(RaftLeaderElectorOperations.WITHDRAW, SERIALIZER::decode, this::withdraw);
        executor.register(RaftLeaderElectorOperations.ANOINT, SERIALIZER::decode, this::anoint, SERIALIZER::encode);
        executor.register(RaftLeaderElectorOperations.PROMOTE, SERIALIZER::decode, this::promote, SERIALIZER::encode);
        executor.register(RaftLeaderElectorOperations.EVICT, SERIALIZER::decode, this::evict);
        // Queries
        executor.register(RaftLeaderElectorOperations.GET_LEADERSHIP, this::getLeadership, SERIALIZER::encode);
    }

    @Override
    public void onExpire(RaftSession session) {
        onSessionEnd(session);
    }

    private void onSessionEnd(RaftSession session) {
        listeners.remove(session.sessionId().id());
        Leadership<byte[]> oldLeadership = leadership();
        cleanup(session);
        Leadership<byte[]> newLeadership = leadership();
        if (!Objects.equal(oldLeadership, newLeadership)) {
            notifyLeadershipChange(oldLeadership, newLeadership);
        }
    }

    protected void cleanup(RaftSession session) {
        Optional<Registration> registration =
            registrations.stream().filter(r -> r.sessionId() == session.sessionId().id()).findFirst();
        if (registration.isPresent()) {
            List<Registration> updatedRegistrations =
                registrations.stream()
                    .filter(r -> r.sessionId() != session.sessionId().id())
                    .collect(Collectors.toList());
            if (leader.sessionId() == session.sessionId().id()) {
                if (!updatedRegistrations.isEmpty()) {
                    this.registrations = updatedRegistrations;
                    this.leader = updatedRegistrations.get(0);
                    this.term = termCounter.incrementAndGet();
                    this.termStartTime = context().wallClock().getTime().unixTimestamp();
                } else {
                    this.registrations = updatedRegistrations;
                    this.leader = null;
                }
            } else {
                this.registrations = updatedRegistrations;
            }
        }
    }

    @Override
    public void onClose(RaftSession session) {
        onSessionEnd(session);
    }

    /**
     * Applies listen commits.
     * @param commit listen commit
     */
    protected void listen(Commit<Void> commit) {
        listeners.put(commit.session().sessionId().id(), commit.session());
    }

    /**
     * Applies unlisten commits.
     * @param commit unlisten commit
     */
    protected void unlisten(Commit<Void> commit) {
        listeners.remove(commit.session().sessionId().id());
    }

    /**
     * Applies an {@link Run} commit.
     * @param commit commit entry
     * @return topic leader. If no previous leader existed this is the node that just entered the race.
     */
    protected Leadership<byte[]> run(Commit<? extends Run> commit) {
        try {
            Leadership<byte[]> oldLeadership = leadership();
            Registration registration = new Registration(commit.value().id(), commit.session().sessionId().id());
            addRegistration(registration);
            Leadership<byte[]> newLeadership = leadership();

            if (!Objects.equal(oldLeadership, newLeadership)) {
                notifyLeadershipChange(oldLeadership, newLeadership);
            }
            return newLeadership;
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    private void notifyLeadershipChange(Leadership<byte[]> previousLeadership, Leadership<byte[]> newLeadership) {
        notifyLeadershipChanges(Lists.newArrayList(new LeadershipEvent<>(Type.CHANGE, previousLeadership, newLeadership)));
    }

    private void notifyLeadershipChanges(List<LeadershipEvent> changes) {
        if (changes.isEmpty()) {
            return;
        }
        listeners.values().forEach(session -> session.publish(RaftLeaderElectorEvents.CHANGE, SERIALIZER::encode, changes));
    }

    private Leadership<byte[]> leadership() {
        return new Leadership<>(leader(), candidates());
    }

    protected Leader<byte[]> leader() {
        if (leader == null) {
            return null;
        } else {
            byte[] leaderId = leader.id();
            return new Leader<>(leaderId, term, termStartTime);
        }
    }

    protected List<byte[]> candidates() {
        return registrations.stream().map(registration -> registration.id()).collect(Collectors.toList());
    }

    protected void addRegistration(Registration registration) {
        if (registrations.stream().noneMatch(r -> Arrays.equals(registration.id(), r.id()))) {
            List<Registration> updatedRegistrations = new LinkedList<>(registrations);
            updatedRegistrations.add(registration);
            boolean newLeader = leader == null;
            this.registrations = updatedRegistrations;
            if (newLeader) {
                this.leader = registration;
                this.term = termCounter.incrementAndGet();
                this.termStartTime = context().wallClock().getTime().unixTimestamp();
            }
        }
    }

    /**
     * Applies a withdraw commit.
     */
    protected void withdraw(Commit<? extends Withdraw> commit) {
        try {
            Leadership<byte[]> oldLeadership = leadership();
            cleanup(commit.value().id());
            Leadership<byte[]> newLeadership = leadership();
            if (!Objects.equal(oldLeadership, newLeadership)) {
                notifyLeadershipChange(oldLeadership, newLeadership);
            }
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    protected void cleanup(byte[] id) {
        Optional<Registration> registration =
            registrations.stream().filter(r -> Arrays.equals(r.id(), id)).findFirst();
        if (registration.isPresent()) {
            List<Registration> updatedRegistrations =
                registrations.stream()
                    .filter(r -> !Arrays.equals(r.id(), id))
                    .collect(Collectors.toList());
            if (Arrays.equals(leader.id(), id)) {
                if (!updatedRegistrations.isEmpty()) {
                    this.registrations = updatedRegistrations;
                    this.leader = updatedRegistrations.get(0);
                    this.term = termCounter.incrementAndGet();
                    this.termStartTime = context().wallClock().getTime().unixTimestamp();
                } else {
                    this.registrations = updatedRegistrations;
                    this.leader = null;
                }
            } else {
                this.registrations = updatedRegistrations;
            }
        }
    }

    /**
     * Applies an {@link Anoint} commit.
     * @param commit anoint commit
     * @return {@code true} if changes were made and the transfer occurred; {@code false} if it did not.
     */
    protected boolean anoint(Commit<? extends Anoint> commit) {
        try {
            byte[] id = commit.value().id();
            Leadership<byte[]> oldLeadership = leadership();
            Registration newLeader = registrations.stream()
                .filter(r -> Arrays.equals(r.id(), id))
                .findFirst()
                .orElse(null);
            if (newLeader != null) {
                this.leader = newLeader;
                this.term = termCounter.incrementAndGet();
                this.termStartTime = context().wallClock().getTime().unixTimestamp();
            }
            Leadership<byte[]> newLeadership = leadership();
            if (!Objects.equal(oldLeadership, newLeadership)) {
                notifyLeadershipChange(oldLeadership, newLeadership);
            }
            return leader != null && Arrays.equals(commit.value().id(), leader.id());
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Applies an {@link Promote} commit.
     * @param commit promote commit
     * @return {@code true} if changes desired end state is achieved.
     */
    protected boolean promote(Commit<? extends Promote> commit) {
        try {
            byte[] id = commit.value().id();
            Leadership<byte[]> oldLeadership = leadership();
            if (oldLeadership == null) {
                return false;
            } else {
                boolean containsCandidate = oldLeadership.candidates().stream()
                    .anyMatch(a -> Arrays.equals(a, id));
                if (!containsCandidate) {
                    return false;
                }
            }
            Registration registration = registrations.stream()
                .filter(r -> Arrays.equals(r.id(), id))
                .findFirst()
                .orElse(null);
            List<Registration> updatedRegistrations = Lists.newArrayList();
            updatedRegistrations.add(registration);
            registrations.stream()
                .filter(r -> !Arrays.equals(r.id(), id))
                .forEach(updatedRegistrations::add);
            this.registrations = updatedRegistrations;
            Leadership<byte[]> newLeadership = leadership();
            if (!Objects.equal(oldLeadership, newLeadership)) {
                notifyLeadershipChange(oldLeadership, newLeadership);
            }
            return true;
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Applies an {@link Evict} commit.
     * @param commit evict commit
     */
    protected void evict(Commit<? extends Evict> commit) {
        try {
            byte[] id = commit.value().id();
            Leadership<byte[]> oldLeadership = leadership();
            Optional<Registration> registration =
                registrations.stream().filter(r -> Arrays.equals(r.id, id)).findFirst();
            if (registration.isPresent()) {
                List<Registration> updatedRegistrations =
                    registrations.stream()
                        .filter(r -> !Arrays.equals(r.id(), id))
                        .collect(Collectors.toList());
                if (Arrays.equals(leader.id(), id)) {
                    if (!updatedRegistrations.isEmpty()) {
                        this.registrations = updatedRegistrations;
                        this.leader = updatedRegistrations.get(0);
                        this.term = termCounter.incrementAndGet();
                        this.termStartTime = context().wallClock().getTime().unixTimestamp();
                    } else {
                        this.registrations = updatedRegistrations;
                        this.leader = null;
                    }
                } else {
                    this.registrations = updatedRegistrations;
                }
            }
            Leadership<byte[]> newLeadership = leadership();
            if (!Objects.equal(oldLeadership, newLeadership)) {
                notifyLeadershipChange(oldLeadership, newLeadership);
            }
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    /**
     * Applies a get leadership commit.
     * @return leader
     */
    protected Leadership<byte[]> getLeadership() {
        try {
            return leadership();
        } catch (Exception e) {
            logger().error("State machine operation failed", e);
            throw Throwables.propagate(e);
        }
    }

    private static class Registration {
        private final byte[] id;
        private final long sessionId;

        protected Registration(byte[] id, long sessionId) {
            this.id = id;
            this.sessionId = sessionId;
        }

        protected byte[] id() {
            return id;
        }

        protected long sessionId() {
            return sessionId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("id", ArraySizeHashPrinter.of(id))
                .add("sessionId", sessionId)
                .toString();
        }
    }
}