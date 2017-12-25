package org.logstash.cluster.protocols.raft.service.impl;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import java.util.function.Function;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.time.LogicalTimestamp;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Server commit.
 */
public class DefaultCommit<T> implements Commit<T> {
    private final long index;
    private final RaftSession session;
    private final long timestamp;
    private final OperationId operation;
    private final T value;

    public DefaultCommit(long index, OperationId operation, T value, RaftSession session, long timestamp) {
        this.index = index;
        this.session = session;
        this.timestamp = timestamp;
        this.operation = operation;
        this.value = value;
    }

    @Override
    public long index() {
        return index;
    }

    @Override
    public RaftSession session() {
        return session;
    }

    @Override
    public LogicalTimestamp logicalTime() {
        return LogicalTimestamp.of(index);
    }

    @Override
    public WallClockTimestamp wallClockTime() {
        return WallClockTimestamp.from(timestamp);
    }

    @Override
    public OperationId operation() {
        return operation;
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public <U> Commit<U> map(Function<T, U> transcoder) {
        return new DefaultCommit<>(index, operation, transcoder.apply(value), session, timestamp);
    }

    @Override
    public Commit<Void> mapToNull() {
        return new DefaultCommit<>(index, operation, null, session, timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Commit.class, index, session.sessionId(), operation);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Commit) {
            Commit commit = (Commit) object;
            return commit.index() == index
                && commit.session().equals(session)
                && commit.operation().equals(operation)
                && Objects.equals(commit.value(), value);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("index", index)
            .add("session", session)
            .add("time", wallClockTime())
            .add("operation", operation)
            .add("value", value instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) value) : value)
            .toString();
    }
}
