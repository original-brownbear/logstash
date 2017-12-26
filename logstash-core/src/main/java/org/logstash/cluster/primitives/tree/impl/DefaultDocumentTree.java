package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTree;
import org.logstash.cluster.primitives.tree.DocumentTreeListener;
import org.logstash.cluster.primitives.tree.DocumentTreeNode;
import org.logstash.cluster.primitives.tree.IllegalDocumentModificationException;
import org.logstash.cluster.primitives.tree.NoSuchDocumentPathException;
import org.logstash.cluster.time.Versioned;

/**
 * Simple implementation of a {@link DocumentTree}.
 * @param <V> tree node value type
 */
public class DefaultDocumentTree<V> implements DocumentTree<V> {

    private static final DocumentPath ROOT_PATH = DocumentPath.from("root");
    final DefaultDocumentTreeNode<V> root;
    private final Supplier<Long> versionSupplier;

    public DefaultDocumentTree() {
        final AtomicLong versionCounter = new AtomicLong(0);
        versionSupplier = versionCounter::incrementAndGet;
        root = new DefaultDocumentTreeNode<>(ROOT_PATH, null, versionSupplier.get(), Ordering.NATURAL, null);
    }

    public DefaultDocumentTree(final Supplier<Long> versionSupplier, final Ordering ordering) {
        root = new DefaultDocumentTreeNode<>(ROOT_PATH, null, versionSupplier.get(), ordering, null);
        this.versionSupplier = versionSupplier;
    }

    DefaultDocumentTree(final Supplier<Long> versionSupplier, final DefaultDocumentTreeNode<V> root) {
        this.root = root;
        this.versionSupplier = versionSupplier;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public Map<String, Versioned<V>> getChildren(final DocumentPath path) {
        final DocumentTreeNode<V> node = getNode(path);
        if (node != null) {
            final Map<String, Versioned<V>> childrenValues = Maps.newLinkedHashMap();
            node.children().forEachRemaining(n -> childrenValues.put(simpleName(n.path()), n.value()));
            return childrenValues;
        }
        throw new NoSuchDocumentPathException();
    }

    @Override
    public Versioned<V> get(final DocumentPath path) {
        final DocumentTreeNode<V> currentNode = getNode(path);
        return currentNode != null ? currentNode.value() : null;
    }

    @Override
    public Versioned<V> set(final DocumentPath path, final V value) {
        checkRootModification(path);
        final DefaultDocumentTreeNode<V> node = getNode(path);
        if (node != null) {
            return node.update(value, versionSupplier.get());
        } else {
            create(path, value);
            return null;
        }
    }

    @Override
    public boolean create(final DocumentPath path, final V value) {
        checkRootModification(path);
        final DocumentTreeNode<V> node = getNode(path);
        if (node != null) {
            return false;
        }
        final DocumentPath parentPath = path.parent();
        final DefaultDocumentTreeNode<V> parentNode = getNode(parentPath);
        if (parentNode == null) {
            throw new IllegalDocumentModificationException();
        }
        parentNode.addChild(simpleName(path), value, versionSupplier.get());
        return true;
    }

    @Override
    public boolean createRecursive(final DocumentPath path, final V value) {
        checkRootModification(path);
        final DocumentTreeNode<V> node = getNode(path);
        if (node != null) {
            return false;
        }
        final DocumentPath parentPath = path.parent();
        if (getNode(parentPath) == null) {
            createRecursive(parentPath, null);
        }
        final DefaultDocumentTreeNode<V> parentNode = getNode(parentPath);
        if (parentNode == null) {
            throw new IllegalDocumentModificationException();
        }
        parentNode.addChild(simpleName(path), value, versionSupplier.get());
        return true;
    }

    @Override
    public boolean replace(final DocumentPath path, final V newValue, final long version) {
        checkRootModification(path);
        final DocumentTreeNode<V> node = getNode(path);
        if (node != null && node.value() != null && node.value().version() == version) {
            set(path, newValue);
            return true;
        }
        return false;
    }

    @Override
    public boolean replace(final DocumentPath path, final V newValue, final V currentValue) {
        checkRootModification(path);
        if (Objects.equals(newValue, currentValue)) {
            return false;
        }
        final DocumentTreeNode<V> node = getNode(path);
        if (node != null && Objects.equals(Versioned.valueOrNull(node.value()), currentValue)) {
            set(path, newValue);
            return true;
        }
        if (node == null && currentValue == null) {
            create(path, newValue);
            return true;
        }
        return false;
    }

    @Override
    public Versioned<V> removeNode(final DocumentPath path) {
        checkRootModification(path);
        final DefaultDocumentTreeNode<V> nodeToRemove = getNode(path);
        if (nodeToRemove == null) {
            throw new NoSuchDocumentPathException();
        }
        if (nodeToRemove.hasChildren()) {
            throw new IllegalDocumentModificationException();
        }
        final DefaultDocumentTreeNode<V> parent = (DefaultDocumentTreeNode<V>) nodeToRemove.parent();
        parent.removeChild(simpleName(path));
        return nodeToRemove.value();
    }

    @Override
    public void removeListener(final DocumentTreeListener<V> listener) {
        // TODO Auto-generated method stub
    }

    @Override
    public DocumentPath root() {
        return ROOT_PATH;
    }

    @Override
    public void addListener(final DocumentPath path, final DocumentTreeListener<V> listener) {
        // TODO Auto-generated method stub
    }

    private static void checkRootModification(final DocumentPath path) {
        if (ROOT_PATH.equals(path)) {
            throw new IllegalDocumentModificationException();
        }
    }

    private DefaultDocumentTreeNode<V> getNode(final DocumentPath path) {
        final Iterator<String> pathElements = path.pathElements().iterator();
        DefaultDocumentTreeNode<V> currentNode = root;
        Preconditions.checkState("root".equals(pathElements.next()), "Path should start with root");
        while (pathElements.hasNext() && currentNode != null) {
            currentNode = (DefaultDocumentTreeNode<V>) currentNode.child(pathElements.next());
        }
        return currentNode;
    }

    private static String simpleName(final DocumentPath path) {
        return path.pathElements().get(path.pathElements().size() - 1);
    }

    @Override
    public void close() {

    }
}
