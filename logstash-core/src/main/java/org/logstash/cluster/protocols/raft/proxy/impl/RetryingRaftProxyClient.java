package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.Throwables;
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxyClient;
import org.logstash.cluster.utils.concurrent.Futures;
import org.logstash.cluster.utils.concurrent.Scheduler;

/**
 * Retrying Copycat session.
 */
public class RetryingRaftProxyClient extends DelegatingRaftProxyClient {

    private static final Logger LOGGER = LogManager.getLogger(RetryingRaftProxyClient.class);

    private final RaftProxyClient client;
    private final Scheduler scheduler;
    private final int maxRetries;
    private final Duration delayBetweenRetries;

    private final Predicate<Throwable> retryableCheck = e ->
        e instanceof ConnectException
            || e instanceof TimeoutException
            || e instanceof ClosedChannelException
            || e instanceof RaftException.QueryFailure
            || e instanceof RaftException.UnknownClient
            || e instanceof RaftException.UnknownSession;

    public RetryingRaftProxyClient(RaftProxyClient delegate, Scheduler scheduler, int maxRetries, Duration delayBetweenRetries) {
        super(delegate);
        this.client = delegate;
        this.scheduler = scheduler;
        this.maxRetries = maxRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    @Override
    public CompletableFuture<byte[]> execute(RaftOperation operation) {
        if (getState() == RaftProxy.State.CLOSED) {
            return Futures.exceptionalFuture(new RaftException.Unavailable("Cluster is unavailable"));
        }
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        execute(operation, 1, future);
        return future;
    }

    private void execute(RaftOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
        client.execute(operation).whenComplete((r, e) -> {
            if (e != null) {
                if (attemptIndex < maxRetries + 1 && retryableCheck.test(Throwables.getRootCause(e))) {
                    LOGGER.debug("Retry attempt ({} of {}). Failure due to {}", attemptIndex, maxRetries, Throwables.getRootCause(e).getClass());
                    scheduleRetry(operation, attemptIndex, future);
                } else {
                    future.completeExceptionally(e);
                }
            } else {
                future.complete(r);
            }
        });
    }

    private void scheduleRetry(RaftOperation operation, int attemptIndex, CompletableFuture<byte[]> future) {
        RaftProxy.State retryState = client.getState();
        scheduler.schedule(delayBetweenRetries, () -> {
            if (retryState == RaftProxy.State.CONNECTED || client.getState() == RaftProxy.State.CONNECTED) {
                execute(operation, attemptIndex + 1, future);
            } else {
                scheduleRetry(operation, attemptIndex, future);
            }
        });
    }
}
