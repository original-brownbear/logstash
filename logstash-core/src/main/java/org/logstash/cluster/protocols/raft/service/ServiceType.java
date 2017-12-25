package org.logstash.cluster.protocols.raft.service;

import org.logstash.cluster.protocols.raft.service.impl.DefaultServiceType;
import org.logstash.cluster.utils.Identifier;

/**
 * Raft service type.
 */
public interface ServiceType extends Identifier<String> {

    /**
     * Creates a new Raft service type identifier.
     * @param name the service type
     * @return the service type identifier
     */
    static ServiceType from(String name) {
        return new DefaultServiceType(name);
    }
}
