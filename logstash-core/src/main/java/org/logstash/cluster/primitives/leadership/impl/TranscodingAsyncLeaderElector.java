package org.logstash.cluster.primitives.leadership.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.primitives.leadership.LeadershipEvent;
import org.logstash.cluster.primitives.leadership.LeadershipEventListener;

/**
 * Transcoding document tree.
 */
public class TranscodingAsyncLeaderElector<V1, V2> implements AsyncLeaderElector<V1> {

    private final AsyncLeaderElector<V2> backingElector;
    private final Function<V1, V2> valueEncoder;
    private final Function<V2, V1> valueDecoder;
    private final Map<LeadershipEventListener<V1>, TranscodingAsyncLeaderElector.InternalLeadershipEventListener> listeners = Maps.newIdentityHashMap();

    public TranscodingAsyncLeaderElector(AsyncLeaderElector<V2> backingElector, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
        this.backingElector = backingElector;
        this.valueEncoder = valueEncoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public String name() {
        return backingElector.name();
    }

    @Override
    public CompletableFuture<Leadership<V1>> run(V1 identifier) {
        return backingElector.run(valueEncoder.apply(identifier)).thenApply(l -> l.map(valueDecoder));
    }

    @Override
    public CompletableFuture<Void> withdraw(V1 identifier) {
        return backingElector.withdraw(valueEncoder.apply(identifier));
    }

    @Override
    public CompletableFuture<Boolean> anoint(V1 identifier) {
        return backingElector.anoint(valueEncoder.apply(identifier));
    }

    @Override
    public CompletableFuture<Void> evict(V1 identifier) {
        return backingElector.evict(valueEncoder.apply(identifier));
    }

    @Override
    public CompletableFuture<Boolean> promote(V1 identifier) {
        return backingElector.promote(valueEncoder.apply(identifier));
    }

    @Override
    public CompletableFuture<Leadership<V1>> getLeadership() {
        return backingElector.getLeadership().thenApply(l -> l.map(valueDecoder));
    }

    @Override
    public CompletableFuture<Void> addListener(LeadershipEventListener<V1> listener) {
        synchronized (listeners) {
            TranscodingAsyncLeaderElector.InternalLeadershipEventListener internalListener =
                listeners.computeIfAbsent(listener, k -> new TranscodingAsyncLeaderElector.InternalLeadershipEventListener(listener));
            return backingElector.addListener(internalListener);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(LeadershipEventListener<V1> listener) {
        synchronized (listeners) {
            TranscodingAsyncLeaderElector.InternalLeadershipEventListener internalListener = listeners.remove(listener);
            if (internalListener != null) {
                return backingElector.removeListener(internalListener);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return backingElector.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("backingTree", backingElector)
            .toString();
    }

    private class InternalLeadershipEventListener implements LeadershipEventListener<V2> {
        private final LeadershipEventListener<V1> listener;

        InternalLeadershipEventListener(LeadershipEventListener<V1> listener) {
            this.listener = listener;
        }

        @Override
        public void onEvent(LeadershipEvent<V2> event) {
            listener.onEvent(new LeadershipEvent<>(
                event.type(),
                event.oldLeadership() != null ? event.oldLeadership().map(valueDecoder) : null,
                event.newLeadership() != null ? event.newLeadership().map(valueDecoder) : null));
        }
    }
}
