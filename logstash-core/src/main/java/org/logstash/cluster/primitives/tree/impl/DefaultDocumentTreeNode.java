package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTreeNode;
import org.logstash.cluster.time.Versioned;

/**
 * A {@code DocumentTree} node.
 */
public class DefaultDocumentTreeNode<V> implements DocumentTreeNode<V> {
    private final DocumentPath key;
    private final Map<String, DocumentTreeNode<V>> children;
    private final Ordering ordering;
    private final DocumentTreeNode<V> parent;
    private Versioned<V> value;

    public DefaultDocumentTreeNode(DocumentPath key,
        V value,
        long version,
        Ordering ordering,
        DocumentTreeNode<V> parent) {
        this.key = Preconditions.checkNotNull(key);
        this.value = new Versioned<>(value, version);
        this.ordering = ordering;
        this.parent = parent;

        switch (ordering) {
            case INSERTION:
                children = Maps.newLinkedHashMap();
                break;
            case NATURAL:
            default:
                children = Maps.newTreeMap();
                break;
        }
    }

    public DocumentTreeNode<V> parent() {
        return parent;
    }

    /**
     * Adds a new child only if one does not exist with the name.
     * @param name relative path name of the child node
     * @param newValue new value to set
     * @param newVersion new version to set
     * @return previous value; can be {@code null} if no child currently exists with that relative path name.
     * a non null return value indicates child already exists and no modification occurred.
     */
    public Versioned<V> addChild(String name, V newValue, long newVersion) {
        DefaultDocumentTreeNode<V> child = (DefaultDocumentTreeNode<V>) children.get(name);
        if (child != null) {
            return child.value();
        }
        children.put(name, new DefaultDocumentTreeNode<>(
            new DocumentPath(name, path()), newValue, newVersion, ordering, this));
        return null;
    }

    @Override
    public DocumentPath path() {
        return key;
    }

    @Override
    public Versioned<V> value() {
        return value;
    }

    @Override
    public DocumentTreeNode<V> child(String name) {
        return children.get(name);
    }

    @Override
    public Iterator<DocumentTreeNode<V>> children() {
        return ImmutableList.copyOf(children.values()).iterator();
    }

    /**
     * Updates the node value.
     * @param newValue new value to set
     * @param newVersion new version to set
     * @return previous value
     */
    public Versioned<V> update(V newValue, long newVersion) {
        Versioned<V> previousValue = value;
        value = new Versioned<>(newValue, newVersion);
        return previousValue;
    }

    /**
     * Removes a child node.
     * @param name the name of child node to be removed
     * @return {@code true} if the child set was modified as a result of this call, {@code false} otherwise
     */
    public boolean removeChild(String name) {
        return children.remove(name) != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DefaultDocumentTreeNode) {
            DefaultDocumentTreeNode<V> that = (DefaultDocumentTreeNode<V>) obj;
            if (this.parent.equals(that.parent)) {
                if (this.children.size() == that.children.size()) {
                    return Sets.symmetricDifference(this.children.keySet(), that.children.keySet()).isEmpty();
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper =
            MoreObjects.toStringHelper(getClass())
                .add("parent", this.parent)
                .add("key", this.key)
                .add("value", this.value);
        for (DocumentTreeNode<V> child : children.values()) {
            helper = helper.add("child", "\n" + child.path().pathElements()
                .get(child.path().pathElements().size() - 1) +
                " : " + child.value());
        }
        return helper.toString();
    }
}