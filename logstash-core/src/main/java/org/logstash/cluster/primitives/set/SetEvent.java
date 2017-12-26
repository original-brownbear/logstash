package org.logstash.cluster.primitives.set;

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * Representation of a DistributedSet update notification.
 * @param <E> set element type
 */
public final class SetEvent<E> {

    private final String name;
    private final SetEvent.Type type;
    private final E entry;

    /**
     * Creates a new event object.
     * @param name set name
     * @param type type of the event
     * @param entry entry the event concerns
     */
    public SetEvent(String name, SetEvent.Type type, E entry) {
        this.name = name;
        this.type = type;
        this.entry = entry;
    }

    /**
     * Returns the set name.
     * @return name of set
     */
    public String name() {
        return name;
    }

    /**
     * Returns the type of the event.
     * @return type of the event
     */
    public SetEvent.Type type() {
        return type;
    }

    /**
     * Returns the entry this event concerns.
     * @return the entry
     */
    public E entry() {
        return entry;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, entry);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SetEvent)) {
            return false;
        }

        SetEvent that = (SetEvent) o;
        return Objects.equals(this.name, that.name) &&
            Objects.equals(this.type, that.type) &&
            Objects.equals(this.entry, that.entry);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("name", name)
            .add("type", type)
            .add("entry", entry)
            .toString();
    }

    /**
     * SetEvent type.
     */
    public enum Type {
        /**
         * Entry added to the set.
         */
        ADD,

        /**
         * Entry removed from the set.
         */
        REMOVE
    }
}
