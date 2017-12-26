package org.logstash.cluster.cluster;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import org.logstash.cluster.event.AbstractEvent;

/**
 * Describes cluster-related event.
 */
public class ClusterEvent extends AbstractEvent<ClusterEvent.Type, Node> {

    /**
     * Creates an event of a given type and for the specified instance and the
     * current time.
     * @param type cluster event type
     * @param instance cluster device subject
     */
    public ClusterEvent(ClusterEvent.Type type, Node instance) {
        super(type, instance);
    }

    /**
     * Creates an event of a given type and for the specified device and time.
     * @param type device event type
     * @param instance event device subject
     * @param time occurrence time
     */
    public ClusterEvent(ClusterEvent.Type type, Node instance, long time) {
        super(type, instance, time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), subject(), time());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ClusterEvent) {
            final ClusterEvent other = (ClusterEvent) obj;
            return Objects.equals(this.type(), other.type()) &&
                Objects.equals(this.subject(), other.subject()) &&
                Objects.equals(this.time(), other.time());
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass())
            .add("type", type())
            .add("subject", subject())
            .add("time", time())
            .toString();
    }

    /**
     * Type of cluster-related events.
     */
    public enum Type {
        /**
         * Signifies that a new cluster instance has been administratively added.
         */
        NODE_ADDED,

        /**
         * Signifies that a cluster instance has been administratively removed.
         */
        NODE_REMOVED,

        /**
         * Signifies that a cluster instance became active.
         */
        NODE_ACTIVATED,

        /**
         * Signifies that a cluster instance became inactive.
         */
        NODE_DEACTIVATED
    }

}
