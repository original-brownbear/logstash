package org.logstash.cluster.primitives.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.logstash.cluster.primitives.AsyncPrimitive;

/**
 * Base class for primitive delegates.
 */
public abstract class DelegatingDistributedPrimitive implements AsyncPrimitive {
    private final AsyncPrimitive primitive;

    public DelegatingDistributedPrimitive(AsyncPrimitive primitive) {
        this.primitive = Preconditions.checkNotNull(primitive);
    }

    @Override
    public String name() {
        return primitive.name();
    }

    @Override
    public Type primitiveType() {
        return primitive.primitiveType();
    }

    @Override
    public void addStatusChangeListener(Consumer<Status> listener) {
        primitive.addStatusChangeListener(listener);
    }

    @Override
    public void removeStatusChangeListener(Consumer<Status> listener) {
        primitive.removeStatusChangeListener(listener);
    }

    @Override
    public Collection<Consumer<Status>> statusChangeListeners() {
        return primitive.statusChangeListeners();
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return primitive.destroy();
    }

    @Override
    public CompletableFuture<Void> close() {
        return primitive.close();
    }

    @Override
    public int hashCode() {
        return Objects.hash(primitive);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof DelegatingDistributedPrimitive
            && primitive.equals(((DelegatingDistributedPrimitive) other).primitive);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("delegate", primitive)
            .toString();
    }
}
