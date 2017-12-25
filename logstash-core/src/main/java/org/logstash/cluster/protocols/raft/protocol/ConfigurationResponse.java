package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server configuration response.
 */
public abstract class ConfigurationResponse extends AbstractRaftResponse {
    protected final long index;
    protected final long term;
    protected final long timestamp;
    protected final Collection<RaftMember> members;

    public ConfigurationResponse(Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
        super(status, error);
        this.index = index;
        this.term = term;
        this.timestamp = timestamp;
        this.members = members;
    }

    /**
     * Returns the response index.
     * @return The response index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns the configuration term.
     * @return The configuration term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns the response configuration time.
     * @return The response time.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the configuration members list.
     * @return The configuration members list.
     */
    public Collection<RaftMember> members() {
        return members;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status, index, term, members);
    }

    @Override
    public boolean equals(Object object) {
        if (getClass().isAssignableFrom(object.getClass())) {
            ConfigurationResponse response = (ConfigurationResponse) object;
            return response.status == status
                && response.index == index
                && response.term == term
                && response.timestamp == timestamp
                && response.members.equals(members);
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("index", index)
                .add("term", term)
                .add("timestamp", timestamp)
                .add("members", members)
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Configuration response builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends ConfigurationResponse> extends AbstractRaftResponse.Builder<T, U> {
        protected long index;
        protected long term;
        protected long timestamp;
        protected Collection<RaftMember> members;

        /**
         * Sets the response index.
         * @param index The response index.
         * @return The response builder.
         * @throws IllegalArgumentException if {@code index} is negative
         */
        @SuppressWarnings("unchecked")
        public T withIndex(long index) {
            Preconditions.checkArgument(index >= 0, "index must be positive");
            this.index = index;
            return (T) this;
        }

        /**
         * Sets the response term.
         * @param term The response term.
         * @return The response builder.
         * @throws IllegalArgumentException if {@code term} is negative
         */
        @SuppressWarnings("unchecked")
        public T withTerm(long term) {
            Preconditions.checkArgument(term >= 0, "term must be positive");
            this.term = term;
            return (T) this;
        }

        /**
         * Sets the response time.
         * @param time The response time.
         * @return The response builder.
         * @throws IllegalArgumentException if {@code time} is negative
         */
        @SuppressWarnings("unchecked")
        public T withTime(long time) {
            Preconditions.checkArgument(time > 0, "time must be positive");
            this.timestamp = time;
            return (T) this;
        }

        /**
         * Sets the response members.
         * @param members The response members.
         * @return The response builder.
         * @throws NullPointerException if {@code members} is null
         */
        @SuppressWarnings("unchecked")
        public T withMembers(Collection<RaftMember> members) {
            this.members = Preconditions.checkNotNull(members, "members cannot be null");
            return (T) this;
        }

        @Override
        protected void validate() {
            super.validate();
            if (status == Status.OK) {
                Preconditions.checkArgument(index >= 0, "index must be positive");
                Preconditions.checkArgument(term >= 0, "term must be positive");
                Preconditions.checkArgument(timestamp > 0, "time must be positive");
                Preconditions.checkNotNull(members, "members cannot be null");
            }
        }
    }
}
