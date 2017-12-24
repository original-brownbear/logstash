package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.impl.DelegatingDistributedPrimitive;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTreeListener;
import org.logstash.cluster.time.Versioned;

/**
 * Document tree that delegates to an underlying instance.
 */
public class DelegatingAsyncDocumentTree<V> extends DelegatingDistributedPrimitive implements AsyncDocumentTree<V> {
    private final AsyncDocumentTree<V> delegateTree;

    public DelegatingAsyncDocumentTree(AsyncDocumentTree<V> delegateTree) {
        super(delegateTree);
        this.delegateTree = delegateTree;
    }

    @Override
    public CompletableFuture<Map<String, Versioned<V>>> getChildren(DocumentPath path) {
        return delegateTree.getChildren(path);
    }

    @Override
    public CompletableFuture<Versioned<V>> get(DocumentPath path) {
        return delegateTree.get(path);
    }

    @Override
    public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
        return delegateTree.set(path, value);
    }

    @Override
    public CompletableFuture<Boolean> create(DocumentPath path, V value) {
        return delegateTree.create(path, value);
    }

    @Override
    public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
        return delegateTree.createRecursive(path, value);
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
        return delegateTree.replace(path, newValue, version);
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
        return delegateTree.replace(path, newValue, currentValue);
    }

    @Override
    public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
        return delegateTree.removeNode(path);
    }

    @Override
    public CompletableFuture<Void> removeListener(DocumentTreeListener<V> listener) {
        return delegateTree.removeListener(listener);
    }

    @Override
    public DocumentPath root() {
        return delegateTree.root();
    }

    @Override
    public CompletableFuture<Void> addListener(DocumentPath path, DocumentTreeListener<V> listener) {
        return delegateTree.addListener(path, listener);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("delegateTree", delegateTree)
            .toString();
    }
}
