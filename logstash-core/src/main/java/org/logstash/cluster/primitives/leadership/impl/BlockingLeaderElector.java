package org.logstash.cluster.primitives.leadership.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.leadership.LeaderElector;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.primitives.leadership.LeadershipEventListener;

/**
 * Default implementation for a {@code LeaderElector} backed by a {@link AsyncLeaderElector}.
 */
public class BlockingLeaderElector<T> extends Synchronous<AsyncLeaderElector<T>> implements LeaderElector<T> {

    private final AsyncLeaderElector<T> asyncElector;
    private final long operationTimeoutMillis;

    public BlockingLeaderElector(AsyncLeaderElector<T> asyncElector, long operationTimeoutMillis) {
        super(asyncElector);
        this.asyncElector = asyncElector;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public Leadership run(T identifier) {
        return complete(asyncElector.run(identifier));
    }

    @Override
    public void withdraw(T identifier) {
        complete(asyncElector.withdraw(identifier));
    }

    @Override
    public boolean anoint(T identifier) {
        return complete(asyncElector.anoint(identifier));
    }

    @Override
    public boolean promote(T identifier) {
        return complete(asyncElector.promote(identifier));
    }

    @Override
    public void evict(T identifier) {
        complete(asyncElector.evict(identifier));
    }

    @Override
    public Leadership getLeadership() {
        return complete(asyncElector.getLeadership());
    }

    @Override
    public void addListener(LeadershipEventListener<T> listener) {
        complete(asyncElector.addListener(listener));
    }

    @Override
    public void removeListener(LeadershipEventListener<T> listener) {
        complete(asyncElector.removeListener(listener));
    }

    private <T> T complete(CompletableFuture<T> future) {
        try {
            return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PrimitiveException.Interrupted();
        } catch (TimeoutException e) {
            throw new PrimitiveException.Timeout();
        } catch (ExecutionException e) {
            throw new PrimitiveException(e.getCause());
        }
    }

    @Override
    public void addStatusChangeListener(Consumer<Status> listener) {
        asyncElector.addStatusChangeListener(listener);
    }

    @Override
    public void removeStatusChangeListener(Consumer<Status> listener) {
        asyncElector.removeStatusChangeListener(listener);
    }

    @Override
    public Collection<Consumer<Status>> statusChangeListeners() {
        return asyncElector.statusChangeListeners();
    }
}
