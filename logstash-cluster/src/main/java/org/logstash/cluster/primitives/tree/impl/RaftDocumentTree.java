/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.logstash.cluster.primitives.tree.impl;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTreeEvent;
import org.logstash.cluster.primitives.tree.DocumentTreeListener;
import org.logstash.cluster.primitives.tree.IllegalDocumentModificationException;
import org.logstash.cluster.primitives.tree.NoSuchDocumentPathException;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.Match;
import org.logstash.cluster.utils.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.logstash.cluster.primitives.tree.impl.DocumentTreeResult.Status.ILLEGAL_MODIFICATION;
import static org.logstash.cluster.primitives.tree.impl.DocumentTreeResult.Status.INVALID_PATH;
import static org.logstash.cluster.primitives.tree.impl.DocumentTreeResult.Status.OK;

/**
 * Distributed resource providing the {@link AsyncDocumentTree} primitive.
 */
public class RaftDocumentTree extends AbstractRaftPrimitive implements AsyncDocumentTree<byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftDocumentTreeOperations.NAMESPACE)
        .register(RaftDocumentTreeEvents.NAMESPACE)
        .build());

    private final Map<DocumentTreeListener<byte[]>, InternalListener> eventListeners = new HashMap<>();

    public RaftDocumentTree(RaftProxy proxy) {
        super(proxy);
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isListening()) {
                proxy.invoke(RaftDocumentTreeOperations.ADD_LISTENER, SERIALIZER::encode, new RaftDocumentTreeOperations.Listen());
            }
        });
        proxy.addEventListener(RaftDocumentTreeEvents.CHANGE, SERIALIZER::decode, this::processTreeUpdates);
    }

    @Override
    public DistributedPrimitive.Type primitiveType() {
        return Type.DOCUMENT_TREE;
    }

    @Override
    public CompletableFuture<Void> destroy() {
        return proxy.invoke(RaftDocumentTreeOperations.CLEAR);
    }

    @Override
    public DocumentPath root() {
        return DocumentPath.ROOT;
    }

    @Override
    public CompletableFuture<Map<String, Versioned<byte[]>>> getChildren(DocumentPath path) {
        return proxy.<RaftDocumentTreeOperations.GetChildren, DocumentTreeResult<Map<String, Versioned<byte[]>>>>invoke(
            RaftDocumentTreeOperations.GET_CHILDREN,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.GetChildren(checkNotNull(path)),
            SERIALIZER::decode)
            .thenCompose(result -> {
                if (result.status() == INVALID_PATH) {
                    return Futures.exceptionalFuture(new NoSuchDocumentPathException());
                } else if (result.status() == ILLEGAL_MODIFICATION) {
                    return Futures.exceptionalFuture(new IllegalDocumentModificationException());
                } else {
                    return CompletableFuture.completedFuture(result);
                }
            }).thenApply(result -> result.result());
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> get(DocumentPath path) {
        return proxy.invoke(RaftDocumentTreeOperations.GET, SERIALIZER::encode, new RaftDocumentTreeOperations.Get(checkNotNull(path)), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> set(DocumentPath path, byte[] value) {
        return proxy.<RaftDocumentTreeOperations.Update, DocumentTreeResult<Versioned<byte[]>>>invoke(RaftDocumentTreeOperations.UPDATE,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.any()),
            SERIALIZER::decode)
            .thenCompose(result -> {
                if (result.status() == INVALID_PATH) {
                    return Futures.exceptionalFuture(new NoSuchDocumentPathException());
                } else if (result.status() == ILLEGAL_MODIFICATION) {
                    return Futures.exceptionalFuture(new IllegalDocumentModificationException());
                } else {
                    return CompletableFuture.completedFuture(result);
                }
            }).thenApply(result -> result.result());
    }

    @Override
    public CompletableFuture<Boolean> create(DocumentPath path, byte[] value) {
        return createInternal(path, value)
            .thenCompose(status -> {
                if (status == ILLEGAL_MODIFICATION) {
                    return Futures.exceptionalFuture(new IllegalDocumentModificationException());
                }
                return CompletableFuture.completedFuture(true);
            });
    }

    @Override
    public CompletableFuture<Boolean> createRecursive(DocumentPath path, byte[] value) {
        return createInternal(path, value)
            .thenCompose(status -> {
                if (status == ILLEGAL_MODIFICATION) {
                    return createRecursive(path.parent(), null)
                        .thenCompose(r -> createInternal(path, value).thenApply(v -> true));
                }
                return CompletableFuture.completedFuture(status == OK);
            });
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, long version) {
        return proxy.<RaftDocumentTreeOperations.Update, DocumentTreeResult<byte[]>>invoke(RaftDocumentTreeOperations.UPDATE,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.Update(checkNotNull(path),
                Optional.ofNullable(newValue),
                Match.any(),
                Match.ifValue(version)), SERIALIZER::decode)
            .thenApply(result -> result.updated());
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, byte[] newValue, byte[] currentValue) {
        return proxy.<RaftDocumentTreeOperations.Update, DocumentTreeResult<byte[]>>invoke(RaftDocumentTreeOperations.UPDATE,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.Update(checkNotNull(path),
                Optional.ofNullable(newValue),
                Match.ifValue(currentValue),
                Match.any()),
            SERIALIZER::decode)
            .thenCompose(result -> {
                if (result.status() == INVALID_PATH) {
                    return Futures.exceptionalFuture(new NoSuchDocumentPathException());
                } else if (result.status() == ILLEGAL_MODIFICATION) {
                    return Futures.exceptionalFuture(new IllegalDocumentModificationException());
                } else {
                    return CompletableFuture.completedFuture(result);
                }
            }).thenApply(result -> result.updated());
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> removeNode(DocumentPath path) {
        if (path.equals(DocumentPath.from("root"))) {
            return Futures.exceptionalFuture(new IllegalDocumentModificationException());
        }
        return proxy.<RaftDocumentTreeOperations.Update, DocumentTreeResult<Versioned<byte[]>>>invoke(RaftDocumentTreeOperations.UPDATE,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.Update(checkNotNull(path), null, Match.any(), Match.ifNotNull()),
            SERIALIZER::decode)
            .thenCompose(result -> {
                if (result.status() == INVALID_PATH) {
                    return Futures.exceptionalFuture(new NoSuchDocumentPathException());
                } else if (result.status() == ILLEGAL_MODIFICATION) {
                    return Futures.exceptionalFuture(new IllegalDocumentModificationException());
                } else {
                    return CompletableFuture.completedFuture(result);
                }
            }).thenApply(result -> result.result());
    }

    @Override
    public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<byte[]> listener) {
        checkNotNull(path);
        checkNotNull(listener);
        InternalListener internalListener = new InternalListener(path, listener, MoreExecutors.directExecutor());
        // TODO: Support API that takes an executor
        if (!eventListeners.containsKey(listener)) {
            return proxy.invoke(RaftDocumentTreeOperations.ADD_LISTENER, SERIALIZER::encode, new RaftDocumentTreeOperations.Listen(path))
                .thenRun(() -> eventListeners.put(listener, internalListener));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeListener(DocumentTreeListener<byte[]> listener) {
        checkNotNull(listener);
        InternalListener internalListener = eventListeners.remove(listener);
        if (internalListener != null && eventListeners.isEmpty()) {
            return proxy.invoke(RaftDocumentTreeOperations.REMOVE_LISTENER, SERIALIZER::encode, new RaftDocumentTreeOperations.Unlisten(internalListener.path))
                .thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<DocumentTreeResult.Status> createInternal(DocumentPath path, byte[] value) {
        return proxy.<RaftDocumentTreeOperations.Update, DocumentTreeResult<byte[]>>invoke(RaftDocumentTreeOperations.UPDATE,
            SERIALIZER::encode,
            new RaftDocumentTreeOperations.Update(checkNotNull(path), Optional.ofNullable(value), Match.any(), Match.ifNull()),
            SERIALIZER::decode)
            .thenApply(result -> result.status());
    }

    private boolean isListening() {
        return !eventListeners.isEmpty();
    }

    private void processTreeUpdates(List<DocumentTreeEvent<byte[]>> events) {
        events.forEach(event -> eventListeners.values().forEach(listener -> listener.event(event)));
    }

    private class InternalListener implements DocumentTreeListener<byte[]> {

        private final DocumentPath path;
        private final DocumentTreeListener<byte[]> listener;
        private final Executor executor;

        public InternalListener(DocumentPath path, DocumentTreeListener<byte[]> listener, Executor executor) {
            this.path = path;
            this.listener = listener;
            this.executor = executor;
        }

        @Override
        public void event(DocumentTreeEvent<byte[]> event) {
            if (event.path().isDescendentOf(path)) {
                executor.execute(() -> listener.event(event));
            }
        }
    }
}
