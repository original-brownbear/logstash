package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Stores a cluster configuration.
 * <p>
 * The {@code ConfigurationEntry} stores information relevant to a single cluster configuration change.
 * Configuration change entries store a collection of {@link RaftMember members} which each represent a
 * server in the cluster. Each time the set of members changes or a property of a single member changes,
 * a new {@code ConfigurationEntry} must be logged for the configuration change.
 */
public class ConfigurationEntry extends TimestampedEntry {
    protected final Collection<RaftMember> members;

    public ConfigurationEntry(long term, long timestamp, Collection<RaftMember> members) {
        super(term, timestamp);
        this.members = members;
    }

    /**
     * Returns the members.
     * @return The members.
     */
    public Collection<RaftMember> members() {
        return members;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("members", members)
            .toString();
    }
}
