package org.logstash.cluster.primitives;

/**
 * Synchronous primitive.
 */
public interface SyncPrimitive extends DistributedPrimitive, AutoCloseable {

    /**
     * Purges state associated with this primitive.
     * <p>
     * Implementations can override and provide appropriate clean up logic for purging
     * any state state associated with the primitive. Whether modifications made within the
     * destroy method have local or global visibility is left unspecified.
     */
    default void destroy() {
    }

}
