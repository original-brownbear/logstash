package org.logstash.cluster.protocols.raft.service;

import java.util.function.Function;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.time.LogicalTimestamp;
import org.logstash.cluster.time.WallClockTimestamp;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 */
public interface Commit<T> {

    /**
     * Returns the commit index.
     * <p>
     * This is the index at which the committed {@link org.logstash.cluster.protocols.raft.operation.RaftOperation} was written in the Raft log.
     * Raft guarantees that this index will be unique for {@link org.logstash.cluster.protocols.raft.operation.RaftOperation} commits and will be the same for all
     * instances of the given operation on all servers in the cluster.
     * <p>
     * For {@link org.logstash.cluster.protocols.raft.operation.RaftOperation} operations, the returned {@code index} may actually be representative of the last committed
     * index in the Raft log since queries are not actually written to disk. Thus, query commits cannot be assumed
     * to have unique indexes.
     * @return The commit index.
     */
    long index();

    /**
     * Returns the session that submitted the operation.
     * <p>
     * The returned {@link RaftSession} is representative of the session that submitted the operation
     * that resulted in this {@link Commit}. The session can be used to {@link RaftSession#publish(org.logstash.cluster.protocols.raft.event.RaftEvent)}
     * event messages to the client.
     * @return The session that created the commit.
     */
    RaftSession session();

    /**
     * Returns the logical time at which the operation was committed.
     * @return The logical commit time.
     */
    LogicalTimestamp logicalTime();

    /**
     * Returns the time at which the operation was committed.
     * <p>
     * The time is representative of the time at which the leader wrote the operation to its log. Because instants
     * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
     * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts. Additionally,
     * commit times are guaranteed to progress monotonically, never going back in time.
     * <p>
     * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
     * upon {@link Commit} times or use the {@link RaftServiceExecutor} for time-based controls.
     * @return The commit time.
     */
    WallClockTimestamp wallClockTime();

    /**
     * Returns the operation identifier.
     * @return the operation identifier
     */
    OperationId operation();

    /**
     * Returns the operation submitted by the client.
     * @return The operation submitted by the client.
     */
    T value();

    /**
     * Converts the commit from one type to another.
     * @param transcoder the transcoder with which to transcode the commit value
     * @param <U> the output commit value type
     * @return the mapped commit
     */
    <U> Commit<U> map(Function<T, U> transcoder);

    /**
     * Converts the commit to a null valued commit.
     * @return the mapped commit
     */
    Commit<Void> mapToNull();

}
