package org.logstash.cluster.primitives.generator;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.SyncPrimitive;

/**
 * Generator for globally unique numeric identifiers.
 */
public interface AtomicIdGenerator extends SyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.ID_GENERATOR;
    }

    /**
     * Gets the next globally unique numeric identifier.
     * @return the next globally unique numeric identifier
     */
    long nextId();

}
