package org.logstash.cluster.protocols.raft.service;

import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.storage.buffer.HeapBytes;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.ThreadContext;

/**
 * Facilitates registration and execution of state machine commands and provides deterministic scheduling.
 * <p>
 * The state machine executor is responsible for managing input to and output from a {@link RaftService}.
 * As operations are committed to the Raft log, the executor is responsible for applying them to the state machine.
 * {@link org.logstash.cluster.protocols.raft.operation.OperationType#COMMAND commands} are guaranteed to be applied to the state machine in the order in which
 * they appear in the Raft log and always in the same thread, so state machines don't have to be thread safe.
 * {@link org.logstash.cluster.protocols.raft.operation.OperationType#QUERY queries} are not generally written to the Raft log and will instead be applied according
 * to their {@link org.logstash.cluster.protocols.raft.ReadConsistency}.
 * </p>
 * State machines can use the executor to provide deterministic scheduling during the execution of command callbacks.
 * <pre>
 *   {@code
 *   private Object putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).close();
 *     });
 *   }
 *   }
 * </pre>
 * As with all state machine callbacks, the executor will ensure scheduled callbacks are executed sequentially and
 * deterministically. As long as state machines schedule callbacks deterministically, callbacks will be executed
 * deterministically. Internally, the state machine executor triggers callbacks based on various timestamps in the
 * Raft log. This means the scheduler is dependent on internal or user-defined operations being written to the log.
 * Prior to the execution of a command, any expired scheduled callbacks will be executed based on the command's
 * logged timestamp.
 * <p>
 * It's important to note that callbacks can only be scheduled during {@link org.logstash.cluster.protocols.raft.operation.RaftOperation} operations or by recursive
 * scheduling. If a state machine attempts to schedule a callback via the executor during the execution of a
 * query, a {@link IllegalStateException} will be thrown. This is because queries are usually only applied
 * on a single state machine within the cluster, and so scheduling callbacks in reaction to query execution would
 * not be deterministic.
 * </p>
 * @see RaftService
 * @see ServiceContext
 */
public interface RaftServiceExecutor extends ThreadContext {

    /**
     * Increments the service clock.
     * @param timestamp the wall clock timestamp
     */
    void tick(WallClockTimestamp timestamp);

    /**
     * Applies the given commit to the executor.
     * @param commit the commit to apply
     * @return the commit result
     */
    byte[] apply(Commit<byte[]> commit);

    /**
     * Registers a operation callback.
     * @param operationId the operation identifier
     * @param callback the operation callback
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default void register(OperationId operationId, Runnable callback) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        handle(operationId, commit -> {
            callback.run();
            return HeapBytes.EMPTY;
        });
    }

    /**
     * Registers a operation callback.
     * @param operationId the operation identifier
     * @param callback the operation callback
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    void handle(OperationId operationId, Function<Commit<byte[]>, byte[]> callback);

    /**
     * Registers a no argument operation callback.
     * @param operationId the operation identifier
     * @param callback the operation callback
     * @param encoder result encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default <R> void register(OperationId operationId, Supplier<R> callback, Function<R, byte[]> encoder) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        handle(operationId, commit -> encoder.apply(callback.get()));
    }

    /**
     * Registers a operation callback.
     * @param operationId the operation identifier
     * @param callback the operation callback
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default void register(OperationId operationId, Consumer<Commit<Void>> callback) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        handle(operationId, commit -> {
            callback.accept(commit.mapToNull());
            return HeapBytes.EMPTY;
        });
    }

    /**
     * Registers a operation callback.
     * @param operationId the operation identifier
     * @param callback the operation callback
     * @param encoder result encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default <R> void register(OperationId operationId, Function<Commit<Void>, R> callback, Function<R, byte[]> encoder) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        Preconditions.checkNotNull(encoder, "encoder cannot be null");
        handle(operationId, commit -> encoder.apply(callback.apply(commit.mapToNull())));
    }

    /**
     * Registers a operation callback.
     * @param operationId the operation identifier
     * @param decoder the operation decoder
     * @param callback the operation callback
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default <T> void register(OperationId operationId, Function<byte[], T> decoder, Consumer<Commit<T>> callback) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        handle(operationId, commit -> {
            callback.accept(commit.map(decoder));
            return HeapBytes.EMPTY;
        });
    }

    /**
     * Registers an operation callback.
     * @param operationId the operation identifier
     * @param decoder the operation decoder
     * @param callback the operation callback
     * @param encoder the output encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    default <T, R> void register(OperationId operationId, Function<byte[], T> decoder, Function<Commit<T>, R> callback, Function<R, byte[]> encoder) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        Preconditions.checkNotNull(encoder, "encoder cannot be null");
        handle(operationId, commit -> encoder.apply(callback.apply(commit.map(decoder))));
    }

    @Override
    default void close() {
    }
}
