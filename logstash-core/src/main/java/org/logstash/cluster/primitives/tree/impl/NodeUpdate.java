package org.logstash.cluster.primitives.tree.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.function.Function;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Map update operation.
 * @param <V> map value type
 */
public final class NodeUpdate<V> {

    private NodeUpdate.Type type;
    private DocumentPath path;
    private V value;
    private long version = -1;

    /**
     * Returns the type of update operation.
     * @return type of update.
     */
    public NodeUpdate.Type type() {
        return type;
    }

    /**
     * Returns the item path being updated.
     * @return item path
     */
    public DocumentPath path() {
        return path;
    }

    /**
     * Returns the new value.
     * @return item's target value.
     */
    public V value() {
        return value;
    }

    /**
     * Returns the expected current version in the database for the key.
     * @return expected version.
     */
    public long version() {
        return version;
    }

    /**
     * Transforms this instance into an instance of different paramterized types.
     * @param valueMapper transcoder to value type
     * @param <T> value type of returned instance
     * @return new instance
     */
    public <T> NodeUpdate<T> map(Function<V, T> valueMapper) {
        return NodeUpdate.<T>builder()
            .withType(type)
            //.withKey(keyMapper.apply(key))
            .withValue(value == null ? null : valueMapper.apply(value))
            .withVersion(version)
            .build();
    }

    /**
     * Creates a new builder instance.
     * @param <V> value type
     * @return builder.
     */
    public static <V> NodeUpdate.Builder<V> builder() {
        return new NodeUpdate.Builder<>();
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, path, value, version);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof NodeUpdate) {
            NodeUpdate that = (NodeUpdate) object;
            return this.type == that.type
                && Objects.equals(this.path, that.path)
                && Objects.equals(this.value, that.value)
                && Objects.equals(this.version, that.version);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("path", path)
            .add("value", value instanceof byte[] ?
                ArraySizeHashPrinter.of((byte[]) value) : value)
            .add("version", version)
            .toString();
    }

    /**
     * Type of database update operation.
     */
    public enum Type {
        /**
         * Creates an entry if the current version matches specified version.
         */
        CREATE_NODE,
        /**
         * Updates an entry if the current version matches specified version.
         */
        UPDATE_NODE,
        /**
         * Deletes an entry if the current version matches specified version.
         */
        DELETE_NODE
    }

    /**
     * NodeUpdate builder.
     * @param <V> value type
     */
    public static final class Builder<V> {

        private final NodeUpdate<V> update = new NodeUpdate<>();

        public NodeUpdate<V> build() {
            validateInputs();
            return update;
        }

        private void validateInputs() {
            Preconditions.checkNotNull(update.type, "type must be specified");
            switch (update.type) {
                case CREATE_NODE:
                    Preconditions.checkNotNull(update.path, "key must be specified");
                    Preconditions.checkNotNull(update.value, "value must be specified.");
                    break;
                case UPDATE_NODE:
                    Preconditions.checkNotNull(update.path, "key must be specified");
                    Preconditions.checkNotNull(update.value, "value must be specified.");
                    Preconditions.checkState(update.version >= 0, "version must be specified");
                    break;
                case DELETE_NODE:
                    Preconditions.checkNotNull(update.path, "key must be specified");
                    Preconditions.checkState(update.version >= 0, "version must be specified");
                    break;
                default:
                    throw new IllegalStateException("Unknown operation type");
            }

        }

        public NodeUpdate.Builder<V> withType(NodeUpdate.Type type) {
            update.type = Preconditions.checkNotNull(type, "type cannot be null");
            return this;
        }

        public NodeUpdate.Builder<V> withPath(DocumentPath key) {
            update.path = Preconditions.checkNotNull(key, "key cannot be null");
            return this;
        }

        public NodeUpdate.Builder<V> withValue(V value) {
            update.value = value;
            return this;
        }

        public NodeUpdate.Builder<V> withVersion(long version) {
            update.version = version;
            return this;
        }
    }
}
