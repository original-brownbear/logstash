package org.logstash.cluster.protocols.raft.cluster;

import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Raft member ID.
 */
public final class MemberId extends AbstractIdentifier<String> implements Comparable<MemberId> {

    /**
     * Constructor for serialization.
     */
    private MemberId() {
        super("");
    }

    /**
     * Creates a new cluster node identifier from the specified string.
     * @param id string identifier
     */
    public MemberId(String id) {
        super(id);
    }

    /**
     * Creates a new cluster node identifier from the specified string.
     * @param id string identifier
     * @return node id
     */
    public static MemberId from(String id) {
        return new MemberId(id);
    }

    @Override
    public int compareTo(MemberId that) {
        return identifier.compareTo(that.identifier);
    }
}
