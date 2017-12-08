package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.Throwables;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentException;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTree;
import org.logstash.cluster.primitives.tree.DocumentTreeListener;
import org.logstash.cluster.time.Versioned;

/**
 * Synchronous wrapper for a {@link AsyncDocumentTree}. All operations are
 * made by making the equivalent calls to a backing {@link AsyncDocumentTree}
 * then blocking until the operations complete or timeout.
 * @param <V> the type of the values
 */
public class BlockingDocumentTree<V> extends Synchronous<AsyncDocumentTree<V>> implements DocumentTree<V> {

    private final AsyncDocumentTree<V> backingTree;
    private final long operationTimeoutMillis;

    public BlockingDocumentTree(AsyncDocumentTree<V> backingTree, long operationTimeoutMillis) {
        super(backingTree);
        this.backingTree = backingTree;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public Map<String, Versioned<V>> getChildren(DocumentPath path) {
        return complete(backingTree.getChildren(path));
    }

    @Override
    public Versioned<V> get(DocumentPath path) {
        return complete(backingTree.get(path));
    }

    @Override
    public Versioned<V> set(DocumentPath path, V value) {
        return complete(backingTree.set(path, value));
    }

    @Override
    public boolean create(DocumentPath path, V value) {
        return complete(backingTree.create(path, value));
    }

    @Override
    public boolean createRecursive(DocumentPath path, V value) {
        return complete(backingTree.createRecursive(path, value));
    }

    @Override
    public boolean replace(DocumentPath path, V newValue, long version) {
        return complete(backingTree.replace(path, newValue, version));
    }

    @Override
    public boolean replace(DocumentPath path, V newValue, V currentValue) {
        return complete(backingTree.replace(path, newValue, currentValue));
    }

    @Override
    public Versioned<V> removeNode(DocumentPath path) {
        return complete(backingTree.removeNode(path));
    }

    @Override
    public void removeListener(DocumentTreeListener<V> listener) {
        complete(backingTree.removeListener(listener));
    }

    @Override
    public void addListener(DocumentTreeListener<V> listener) {
        complete(backingTree.addListener(listener));
    }

    @Override
    public DocumentPath root() {
        return backingTree.root();
    }

    @Override
    public void addListener(DocumentPath path, DocumentTreeListener<V> listener) {
        complete(backingTree.addListener(path, listener));
    }

    private <T> T complete(CompletableFuture<T> future) {
        try {
            return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DocumentException.Interrupted();
        } catch (TimeoutException e) {
            throw new DocumentException.Timeout(name());
        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e.getCause());
            throw new PrimitiveException(e.getCause());
        }
    }
}
