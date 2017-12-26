package org.logstash.cluster.primitives.lock.impl;

import com.google.common.base.MoreObjects;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.utils.concurrent.Scheduled;

/**
 * Raft atomic value service.
 */
public class RaftDistributedLockService extends AbstractRaftService {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftDistributedLockOperations.NAMESPACE)
        .register(RaftDistributedLockEvents.NAMESPACE)
        .register(RaftDistributedLockService.LockHolder.class)
        .build());
    private final Map<Long, Scheduled> timers = new HashMap<>();
    private RaftDistributedLockService.LockHolder lock;
    private Queue<RaftDistributedLockService.LockHolder> queue = new ArrayDeque<>();

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(RaftDistributedLockOperations.LOCK, SERIALIZER::decode, this::lock);
        executor.register(RaftDistributedLockOperations.UNLOCK, SERIALIZER::decode, this::unlock);
    }

    @Override
    public void onExpire(RaftSession session) {
        releaseSession(session);
    }

    @Override
    public void onClose(RaftSession session) {
        releaseSession(session);
    }

    private void releaseSession(RaftSession session) {
        if (lock.session == session.sessionId().id()) {
            lock = queue.poll();
            while (lock != null) {
                if (lock.session == session.sessionId().id()) {
                    lock = queue.poll();
                } else {
                    Scheduled timer = timers.remove(lock.index);
                    if (timer != null) {
                        timer.cancel();
                    }

                    RaftSession lockSession = sessions().getSession(lock.session);
                    if (lockSession == null || lockSession.getState() == RaftSession.State.EXPIRED || lockSession.getState() == RaftSession.State.CLOSED) {
                        lock = queue.poll();
                    } else {
                        lockSession.publish(RaftDistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(lock.id, lock.index));
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeObject(lock, SERIALIZER::encode);
        writer.writeObject(queue, SERIALIZER::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        lock = reader.readObject(SERIALIZER::decode);
        queue = reader.readObject(SERIALIZER::decode);
        timers.values().forEach(Scheduled::cancel);
        timers.clear();
        for (RaftDistributedLockService.LockHolder holder : queue) {
            if (holder.expire > 0) {
                timers.put(holder.index, scheduler().schedule(Duration.ofMillis(holder.expire - context().wallClock().getTime().unixTimestamp()), () -> {
                    timers.remove(holder.index);
                    queue.remove(holder);
                    RaftSession session = sessions().getSession(holder.session);
                    if (session != null && session.getState().active()) {
                        session.publish(RaftDistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(holder.id, holder.index));
                    }
                }));
            }
        }
    }

    /**
     * Applies a lock commit.
     */
    protected void lock(Commit<RaftDistributedLockOperations.Lock> commit) {
        if (lock == null) {
            lock = new RaftDistributedLockService.LockHolder(
                commit.value().id(),
                commit.index(),
                commit.session().sessionId().id(),
                0);
            commit.session().publish(RaftDistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
        } else if (commit.value().timeout() == 0) {
            commit.session().publish(RaftDistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
        } else if (commit.value().timeout() > 0) {
            RaftDistributedLockService.LockHolder holder = lock = new RaftDistributedLockService.LockHolder(
                commit.value().id(),
                commit.index(),
                commit.session().sessionId().id(),
                context().wallClock().getTime().unixTimestamp() + commit.value().timeout());
            queue.add(holder);
            timers.put(commit.index(), scheduler().schedule(Duration.ofMillis(commit.value().timeout()), () -> {
                timers.remove(commit.index());
                queue.remove(holder);
                if (commit.session().getState().active()) {
                    commit.session().publish(RaftDistributedLockEvents.FAIL, SERIALIZER::encode, new LockEvent(commit.value().id(), commit.index()));
                }
            }));
        } else {
            RaftDistributedLockService.LockHolder holder = new RaftDistributedLockService.LockHolder(
                commit.value().id(),
                commit.index(),
                commit.session().sessionId().id(),
                0);
            queue.add(holder);
        }
    }

    /**
     * Applies an unlock commit.
     */
    protected void unlock(Commit<RaftDistributedLockOperations.Unlock> commit) {
        if (lock != null) {
            if (lock.session != commit.session().sessionId().id()) {
                return;
            }

            lock = queue.poll();
            while (lock != null) {
                Scheduled timer = timers.remove(lock.index);
                if (timer != null) {
                    timer.cancel();
                }

                RaftSession session = sessions().getSession(lock.session);
                if (session == null || session.getState() == RaftSession.State.EXPIRED || session.getState() == RaftSession.State.CLOSED) {
                    lock = queue.poll();
                } else {
                    session.publish(RaftDistributedLockEvents.LOCK, SERIALIZER::encode, new LockEvent(lock.id, commit.index()));
                    break;
                }
            }
        }
    }

    private static class LockHolder {
        private final int id;
        private final long index;
        private final long session;
        private final long expire;

        public LockHolder(int id, long index, long session, long expire) {
            this.id = id;
            this.index = index;
            this.session = session;
            this.expire = expire;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("index", index)
                .add("session", session)
                .add("expire", expire)
                .toString();
        }
    }
}
