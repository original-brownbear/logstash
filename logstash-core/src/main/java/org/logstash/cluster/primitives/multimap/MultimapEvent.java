package org.logstash.cluster.primitives.multimap;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * Representation of a ConsistentMultimap update notification.
 * @param <K> key type
 * @param <V> value type
 */
public class MultimapEvent<K, V> {

    private final String name;
    private final Type type;
    private final K key;
    private final V newValue;
    private final V oldValue;

    /**
     * Creates a new event object.
     * @param name map name
     * @param key key the event concerns
     * @param newValue new value key is mapped to
     * @param oldValue previous value that was mapped to the key
     */
    public MultimapEvent(String name, K key, V newValue, V oldValue) {
        this.name = name;
        this.key = key;
        this.newValue = newValue;
        this.oldValue = oldValue;
        this.type = newValue != null ? Type.INSERT : Type.REMOVE;
    }

    /**
     * Returns the map name.
     * @return name of map
     */
    public String name() {
        return name;
    }

    /**
     * Returns the type of the event.
     * @return the type of event
     */
    public Type type() {
        return type;
    }

    /**
     * Returns the key this event concerns.
     * @return the key
     */
    public K key() {
        return key;
    }

    /**
     * Returns the new value in the map associated with the key.
     * If {@link #type()} returns {@code REMOVE},
     * this method will return {@code null}.
     * @return the new value for key
     */
    public V newValue() {
        return newValue;
    }

    /**
     * Returns the old value that was associated with the key.
     * If {@link #type()} returns {@code INSERT}, this method will return
     * {@code null}.
     * @return the old value that was mapped to the key
     */
    public V oldValue() {
        return oldValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, key, newValue, oldValue);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MultimapEvent)) {
            return false;
        }

        MultimapEvent<K, V> that = (MultimapEvent) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.type, that.type) &&
            Objects.equals(this.key, that.key) &&
            Objects.equals(this.newValue, that.newValue) &&
            Objects.equals(this.oldValue, that.oldValue);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("name", name)
            .add("type", type)
            .add("key", key)
            .add("newValue", newValue)
            .add("oldValue", oldValue)
            .toString();
    }

    /**
     * MultimapEvent type.
     */
    public enum Type {
        /**
         * Entry inserted into the map.
         */
        INSERT,

        /**
         * Entry removed from map.
         */
        REMOVE
    }
}
