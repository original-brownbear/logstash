package org.logstash.cluster.primitives.lock.impl;

import com.google.common.base.MoreObjects;

/**
 * Locked event.
 */
public class LockEvent {
    private final int id;
    private final long version;

    public LockEvent() {
        this(0, 0);
    }

    public LockEvent(int id, long version) {
        this.id = id;
        this.version = version;
    }

    /**
     * Returns the lock ID.
     * @return The lock ID.
     */
    public int id() {
        return id;
    }

    /**
     * Returns the lock version.
     * @return The lock version.
     */
    public long version() {
        return version;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("version", version)
            .toString();
    }
}
