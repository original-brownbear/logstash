package org.logstash.cluster.protocols.raft.event;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Raft event.
 */
public class RaftEvent {
    private final EventType type;
    private final byte[] value;

    protected RaftEvent() {
        this.type = null;
        this.value = null;
    }

    public RaftEvent(EventType type, byte[] value) {
        this.type = type;
        this.value = value;
    }

    /**
     * Returns the event type identifier.
     * @return the event type identifier
     */
    public EventType type() {
        return type;
    }

    /**
     * Returns the event value.
     * @return the event value
     */
    public byte[] value() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), type, value);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof RaftEvent) {
            RaftEvent event = (RaftEvent) object;
            return Objects.equals(event.type, type) && Objects.equals(event.value, value);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("value", ArraySizeHashPrinter.of(value))
            .toString();
    }
}
