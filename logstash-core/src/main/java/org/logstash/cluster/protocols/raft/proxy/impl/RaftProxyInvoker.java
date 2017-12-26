package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.Preconditions;
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.OperationRequest;
import org.logstash.cluster.protocols.raft.protocol.OperationResponse;
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.utils.concurrent.ThreadContext;

/**
 * Session operation submitter.
 */
final class RaftProxyInvoker {
    private static final int[] FIBONACCI = {1, 1, 2, 3, 5};
    private static final Predicate<Throwable> EXCEPTION_PREDICATE = e ->
        e instanceof RaftException.ProtocolException
            || e instanceof ConnectException
            || e instanceof TimeoutException
            || e instanceof ClosedChannelException;
    private static final Predicate<Throwable> CLOSED_PREDICATE = e ->
        e instanceof RaftException.ClosedSession
            || e instanceof RaftException.UnknownSession;

    private final RaftProxyConnection leaderConnection;
    private final RaftProxyConnection sessionConnection;
    private final RaftProxyState state;
    private final RaftProxySequencer sequencer;
    private final RaftProxyManager manager;
    private final ThreadContext context;
    private final Map<Long, RaftProxyInvoker.OperationAttempt> attempts = new LinkedHashMap<>();
    private final AtomicLong keepAliveIndex = new AtomicLong();

    public RaftProxyInvoker(
        final RaftProxyConnection leaderConnection,
        final RaftProxyConnection sessionConnection,
        final RaftProxyState state,
        final RaftProxySequencer sequencer,
        final RaftProxyManager manager,
        final ThreadContext context) {
        this.leaderConnection = Preconditions.checkNotNull(leaderConnection, "leaderConnection");
        this.sessionConnection = Preconditions.checkNotNull(sessionConnection, "sessionConnection");
        this.state = Preconditions.checkNotNull(state, "state");
        this.sequencer = Preconditions.checkNotNull(sequencer, "sequencer");
        this.manager = Preconditions.checkNotNull(manager, "manager");
        this.context = Preconditions.checkNotNull(context, "context cannot be null");
    }

    /**
     * Submits a operation to the cluster.
     * @param operation The operation to submit.
     * @return A completable future to be completed once the command has been submitted.
     */
    public CompletableFuture<byte[]> invoke(final RaftOperation operation) {
        final CompletableFuture<byte[]> future = new CompletableFuture<>();
        switch (operation.id().type()) {
            case COMMAND:
                context.execute(() -> invokeCommand(operation, future));
                break;
            case QUERY:
                context.execute(() -> invokeQuery(operation, future));
                break;
            default:
                throw new IllegalArgumentException("Unknown operation type " + operation.id().type());
        }
        return future;
    }

    /**
     * Submits a command to the cluster.
     */
    private void invokeCommand(final RaftOperation operation, final CompletableFuture<byte[]> future) {
        final CommandRequest request = CommandRequest.builder()
            .withSession(state.getSessionId().id())
            .withSequence(state.nextCommandRequest())
            .withOperation(operation)
            .build();
        invokeCommand(request, future);
    }

    /**
     * Submits a command request to the cluster.
     */
    private void invokeCommand(final CommandRequest request, final CompletableFuture<byte[]> future) {
        invoke(new RaftProxyInvoker.CommandAttempt(sequencer.nextRequest(), request, future));
    }

    /**
     * Submits an operation attempt.
     * @param attempt The attempt to submit.
     */
    private <T extends OperationRequest, U extends OperationResponse> void invoke(final RaftProxyInvoker.OperationAttempt<T, U> attempt) {
        if (state.getState() == RaftProxy.State.CLOSED) {
            attempt.fail(new RaftException.ClosedSession("session closed"));
        } else {
            attempts.put(attempt.sequence, attempt);
            attempt.send();
            attempt.future.whenComplete((r, e) -> attempts.remove(attempt.sequence));
        }
    }

    /**
     * Submits a query to the cluster.
     */
    private void invokeQuery(final RaftOperation operation, final CompletableFuture<byte[]> future) {
        final QueryRequest request = QueryRequest.builder()
            .withSession(state.getSessionId().id())
            .withSequence(state.getCommandRequest())
            .withOperation(operation)
            .withIndex(state.getResponseIndex())
            .build();
        invokeQuery(request, future);
    }

    /**
     * Submits a query request to the cluster.
     */
    private void invokeQuery(final QueryRequest request, final CompletableFuture<byte[]> future) {
        invoke(new QueryAttempt(sequencer.nextRequest(), request, future));
    }

    /**
     * Resubmits commands starting after the given sequence number.
     * <p>
     * The sequence number from which to resend commands is the <em>request</em> sequence number,
     * not the client-side sequence number. We resend only commands since queries cannot be reliably
     * resent without losing linearizable semantics. Commands are resent by iterating through all pending
     * operation attempts and retrying commands where the sequence number is greater than the given
     * {@code commandSequence} number and the attempt number is less than or equal to the version.
     */
    private void resubmit(final long commandSequence, final RaftProxyInvoker.OperationAttempt<?, ?> attempt) {
        // If the client's response sequence number is greater than the given command sequence number,
        // the cluster likely has a new leader, and we need to reset the sequencing in the leader by
        // sending a keep-alive request.
        // Ensure that the client doesn't resubmit many concurrent KeepAliveRequests by tracking the last
        // keep-alive response sequence number and only resubmitting if the sequence number has changed.
        final long responseSequence = state.getCommandResponse();
        if (commandSequence < responseSequence && keepAliveIndex.get() != responseSequence) {
            keepAliveIndex.set(responseSequence);
            manager.resetIndexes(state.getSessionId()).whenCompleteAsync((result, error) -> {
                if (error == null) {
                    resubmit(responseSequence, attempt);
                } else {
                    keepAliveIndex.set(0);
                    attempt.retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt.attempt - 1, FIBONACCI.length - 1)]));
                }
            }, context);
        } else {
            for (final Map.Entry<Long, RaftProxyInvoker.OperationAttempt> entry : attempts.entrySet()) {
                final RaftProxyInvoker.OperationAttempt operation = entry.getValue();
                if (operation instanceof RaftProxyInvoker.CommandAttempt && operation.request.sequenceNumber() > commandSequence && operation.attempt <= attempt.attempt) {
                    operation.retry();
                }
            }
        }
    }

    /**
     * Closes the submitter.
     * @return A completable future to be completed with a list of pending operations.
     */
    public CompletableFuture<Void> close() {
        for (final RaftProxyInvoker.OperationAttempt attempt : new ArrayList<>(attempts.values())) {
            attempt.fail(new RaftException.ClosedSession("session closed"));
        }
        attempts.clear();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Operation attempt.
     */
    private abstract class OperationAttempt<T extends OperationRequest, U extends OperationResponse> implements BiConsumer<U, Throwable> {
        protected final long sequence;
        protected final int attempt;
        protected final T request;
        protected final CompletableFuture<byte[]> future;

        protected OperationAttempt(final long sequence, final int attempt, final T request, final CompletableFuture<byte[]> future) {
            this.sequence = sequence;
            this.attempt = attempt;
            this.request = request;
            this.future = future;
        }

        /**
         * Sends the attempt.
         */
        protected abstract void send();

        /**
         * Completes the operation successfully.
         * @param response The operation response.
         */
        protected abstract void complete(U response);

        /**
         * Fails the attempt.
         */
        public void fail() {
            fail(defaultException());
        }

        /**
         * Returns a new instance of the default exception for the operation.
         * @return A default exception for the operation.
         */
        protected abstract Throwable defaultException();

        /**
         * Fails the attempt with the given exception.
         * @param t The exception with which to fail the attempt.
         */
        public void fail(final Throwable t) {
            complete(t);
        }

        /**
         * Completes the operation with an exception.
         * @param error The completion exception.
         */
        protected void complete(final Throwable error) {
            sequence(null, () -> future.completeExceptionally(error));
        }

        /**
         * Runs the given callback in proper sequence.
         * @param response The operation response.
         * @param callback The callback to run in sequence.
         */
        protected final void sequence(final OperationResponse response, final Runnable callback) {
            sequencer.sequenceResponse(sequence, response, callback);
        }

        /**
         * Immediately retries the attempt.
         */
        public void retry() {
            context.execute(() -> invoke(next()));
        }

        /**
         * Returns the next instance of the attempt.
         * @return The next instance of the attempt.
         */
        protected abstract RaftProxyInvoker.OperationAttempt<T, U> next();

        /**
         * Retries the attempt after the given duration.
         * @param after The duration after which to retry the attempt.
         */
        public void retry(final Duration after) {
            context.schedule(after, () -> invoke(next()));
        }
    }

    /**
     * Command operation attempt.
     */
    private final class CommandAttempt extends RaftProxyInvoker.OperationAttempt<CommandRequest, CommandResponse> {
        private final long time = System.currentTimeMillis();

        public CommandAttempt(final long sequence, final CommandRequest request, final CompletableFuture<byte[]> future) {
            super(sequence, 1, request, future);
        }

        public CommandAttempt(final long sequence, final int attempt, final CommandRequest request, final CompletableFuture<byte[]> future) {
            super(sequence, attempt, request, future);
        }

        @Override
        protected void send() {
            leaderConnection.command(request).whenComplete(this);
        }

        @Override
        public void accept(final CommandResponse response, final Throwable error) {
            if (error == null) {
                if (response.status() == RaftResponse.Status.OK) {
                    complete(response);
                }
                // COMMAND_ERROR indicates that the command was received by the leader out of sequential order.
                // We need to resend commands starting at the provided lastSequence number.
                else if (response.error().type() == RaftError.Type.COMMAND_FAILURE) {
                    resubmit(response.lastSequenceNumber(), this);
                }
                // If an APPLICATION_ERROR occurred, complete the request exceptionally with the error message.
                else if (response.error().type() == RaftError.Type.APPLICATION_ERROR) {
                    complete(response.error().createException());
                }
                // If the client is unknown by the cluster, close the session and complete the operation exceptionally.
                else if (response.error().type() == RaftError.Type.UNKNOWN_CLIENT
                    || response.error().type() == RaftError.Type.UNKNOWN_SESSION
                    || response.error().type() == RaftError.Type.UNKNOWN_SERVICE
                    || response.error().type() == RaftError.Type.CLOSED_SESSION) {
                    state.setState(RaftProxy.State.CLOSED);
                    complete(response.error().createException());
                }
                // For all other errors, use fibonacci backoff to resubmit the command.
                else {
                    retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
                }
            } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
                if (error instanceof ConnectException || error.getCause() instanceof ConnectException) {
                    leaderConnection.reset(null, leaderConnection.members());
                }
                retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
            } else {
                fail(error);
            }
        }        @Override
        protected RaftProxyInvoker.OperationAttempt<CommandRequest, CommandResponse> next() {
            return new RaftProxyInvoker.CommandAttempt(sequence, this.attempt + 1, request, future);
        }

        @Override
        protected Throwable defaultException() {
            return new RaftException.CommandFailure("failed to complete command");
        }



        @Override
        public void fail(final Throwable cause) {
            super.fail(cause);

            // If the session has been closed, update the client's state.
            if (CLOSED_PREDICATE.test(cause)) {
                state.setState(RaftProxy.State.CLOSED);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void complete(final CommandResponse response) {
            sequence(response, () -> {
                state.setCommandResponse(request.sequenceNumber());
                state.setResponseIndex(response.index());
                future.complete(response.result());
            });
        }
    }

    /**
     * Query operation attempt.
     */
    private final class QueryAttempt extends RaftProxyInvoker.OperationAttempt<QueryRequest, QueryResponse> {
        public QueryAttempt(final long sequence, final QueryRequest request, final CompletableFuture<byte[]> future) {
            super(sequence, 1, request, future);
        }

        public QueryAttempt(final long sequence, final int attempt, final QueryRequest request, final CompletableFuture<byte[]> future) {
            super(sequence, attempt, request, future);
        }

        @Override
        protected void send() {
            sessionConnection.query(request).whenComplete(this);
        }

        @Override
        protected RaftProxyInvoker.OperationAttempt<QueryRequest, QueryResponse> next() {
            return new QueryAttempt(sequence, this.attempt + 1, request, future);
        }

        @Override
        protected Throwable defaultException() {
            return new RaftException.QueryFailure("failed to complete query");
        }

        @Override
        public void accept(final QueryResponse response, final Throwable error) {
            if (error == null) {
                if (response.status() == RaftResponse.Status.OK) {
                    complete(response);
                } else {
                    complete(response.error().createException());
                }
            } else {
                fail(error);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void complete(final QueryResponse response) {
            sequence(response, () -> {
                state.setResponseIndex(response.index());
                future.complete(response.result());
            });
        }
    }

}
