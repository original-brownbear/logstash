package org.logstash.cluster.protocols.raft.storage.system;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Represents a persisted server configuration.
 */
public class Configuration {
    private final long index;
    private final long term;
    private final long time;
    private final Collection<RaftMember> members;

    public Configuration(long index, long term, long time, Collection<RaftMember> members) {
        Preconditions.checkArgument(time > 0, "time must be positive");
        Preconditions.checkNotNull(members, "members cannot be null");
        this.index = index;
        this.term = term;
        this.time = time;
        this.members = members;
    }

    /**
     * Returns the configuration index.
     * <p>
     * The index is the index of the {@link org.logstash.cluster.protocols.raft.storage.log.entry.ConfigurationEntry ConfigurationEntry}
     * which resulted in this configuration.
     * @return The configuration index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns the configuration term.
     * <p>
     * The term is the term of the leader at the time the configuration change was committed.
     * @return The configuration term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns the configuration time.
     * @return The time at which the configuration was committed.
     */
    public long time() {
        return time;
    }

    /**
     * Returns the cluster membership for this configuration.
     * @return The cluster membership.
     */
    public Collection<RaftMember> members() {
        return members;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("index", index)
            .add("time", new TimestampPrinter(time))
            .add("members", members)
            .toString();
    }

}
