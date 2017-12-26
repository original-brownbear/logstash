package org.logstash.cluster.primitives.tree;

import com.google.common.base.MoreObjects;
import java.util.Optional;
import org.logstash.cluster.time.Versioned;

/**
 * A document tree modification event.
 * @param <V> tree node value type
 */
public class DocumentTreeEvent<V> {

    private final DocumentPath path;
    private final DocumentTreeEvent.Type type;
    private final Optional<Versioned<V>> newValue;
    private final Optional<Versioned<V>> oldValue;

    @SuppressWarnings("unused")
    private DocumentTreeEvent() {
        this.path = null;
        this.type = null;
        this.newValue = null;
        this.oldValue = null;
    }

    /**
     * Constructs a new {@code DocumentTreeEvent}.
     * @param path path to the node
     * @param type type of change
     * @param newValue optional new value; will be empty if node was deleted
     * @param oldValue optional old value; will be empty if node was created
     */
    public DocumentTreeEvent(DocumentPath path,
        DocumentTreeEvent.Type type,
        Optional<Versioned<V>> newValue,
        Optional<Versioned<V>> oldValue) {
        this.path = path;
        this.type = type;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    /**
     * Returns the path to the changed node.
     * @return node path
     */
    public DocumentPath path() {
        return path;
    }

    /**
     * Returns the change type.
     * @return change type
     */
    public DocumentTreeEvent.Type type() {
        return type;
    }

    /**
     * Returns the new value.
     * @return optional new value; will be empty if node was deleted
     */
    public Optional<Versioned<V>> newValue() {
        return newValue;
    }

    /**
     * Returns the old value.
     * @return optional old value; will be empty if node was created
     */
    public Optional<Versioned<V>> oldValue() {
        return oldValue;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("path", path)
            .add("type", type)
            .add("newValue", newValue)
            .add("oldValue", oldValue)
            .toString();
    }

    /**
     * Nature of document tree node change.
     */
    public enum Type {

        /**
         * Signifies node being created.
         */
        CREATED,

        /**
         * Signifies the value of an existing node being updated.
         */
        UPDATED,

        /**
         * Signifies an existing node being deleted.
         */
        DELETED
    }
}
