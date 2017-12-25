package org.logstash.cluster.protocols.raft.service;

import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Snapshot identifier.
 */
public class ServiceId extends AbstractIdentifier<Long> {

    public ServiceId(Long value) {
        super(value);
    }

    /**
     * Creates a snapshot ID from the given string.
     * @param id the string from which to create the identifier
     * @return the snapshot identifier
     */
    public static ServiceId from(String id) {
        return from(Long.parseLong(id));
    }

    /**
     * Creates a snapshot ID from the given number.
     * @param id the number from which to create the identifier
     * @return the snapshot identifier
     */
    public static ServiceId from(long id) {
        return new ServiceId(id);
    }
}
