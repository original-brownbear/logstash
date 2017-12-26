package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.service.ServiceType;

/**
 * Open session request.
 */
public class OpenSessionRequest extends AbstractRaftRequest {

    private final String member;
    private final String name;
    private final String typeName;
    private final ReadConsistency readConsistency;
    private final long minTimeout;
    private final long maxTimeout;

    public OpenSessionRequest(String member, String name, String typeName, ReadConsistency readConsistency, long minTimeout, long maxTimeout) {
        this.member = member;
        this.name = name;
        this.typeName = typeName;
        this.readConsistency = readConsistency;
        this.minTimeout = minTimeout;
        this.maxTimeout = maxTimeout;
    }

    /**
     * Returns a new open session request builder.
     * @return A new open session request builder.
     */
    public static OpenSessionRequest.Builder builder() {
        return new OpenSessionRequest.Builder();
    }

    /**
     * Returns the client node identifier.
     * @return The client node identifier.
     */
    public String member() {
        return member;
    }

    /**
     * Returns the state machine name.
     * @return The state machine name.
     */
    public String serviceName() {
        return name;
    }

    /**
     * Returns the state machine type;
     * @return The state machine type.
     */
    public String serviceType() {
        return typeName;
    }

    /**
     * Returns the session read consistency level.
     * @return The session's read consistency.
     */
    public ReadConsistency readConsistency() {
        return readConsistency;
    }

    /**
     * Returns the minimum session timeout.
     * @return The minimum session timeout.
     */
    public long minTimeout() {
        return minTimeout;
    }

    /**
     * Returns the maximum session timeout.
     * @return The maximum session timeout.
     */
    public long maxTimeout() {
        return maxTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), name, typeName, minTimeout, maxTimeout);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof OpenSessionRequest) {
            OpenSessionRequest request = (OpenSessionRequest) object;
            return request.member.equals(member)
                && request.name.equals(name)
                && request.typeName.equals(typeName)
                && request.readConsistency == readConsistency
                && request.minTimeout == minTimeout
                && request.maxTimeout == maxTimeout;
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("node", member)
            .add("serviceName", name)
            .add("serviceType", typeName)
            .add("readConsistency", readConsistency)
            .add("minTimeout", minTimeout)
            .add("maxTimeout", maxTimeout)
            .toString();
    }

    /**
     * Open session request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<OpenSessionRequest.Builder, OpenSessionRequest> {
        private String memberId;
        private String serviceName;
        private String serviceType;
        private ReadConsistency readConsistency = ReadConsistency.LINEARIZABLE;
        private long minTimeout;
        private long maxTimeout;

        /**
         * Sets the client node identifier.
         * @param member The client node identifier.
         * @return The open session request builder.
         * @throws NullPointerException if {@code node} is {@code null}
         */
        public OpenSessionRequest.Builder withMemberId(MemberId member) {
            this.memberId = Preconditions.checkNotNull(member, "node cannot be null").id();
            return this;
        }

        /**
         * Sets the service name.
         * @param serviceName The service name.
         * @return The open session request builder.
         * @throws NullPointerException if {@code serviceName} is {@code null}
         */
        public OpenSessionRequest.Builder withServiceName(String serviceName) {
            this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName cannot be null");
            return this;
        }

        /**
         * Sets the service type name.
         * @param serviceType The service type name.
         * @return The open session request builder.
         * @throws NullPointerException if {@code serviceType} is {@code null}
         */
        public OpenSessionRequest.Builder withServiceType(ServiceType serviceType) {
            this.serviceType = Preconditions.checkNotNull(serviceType, "serviceType cannot be null").id();
            return this;
        }

        /**
         * Sets the session read consistency.
         * @param readConsistency the session read consistency
         * @return the session request builder
         * @throws NullPointerException if the {@code readConsistency} is null
         */
        public OpenSessionRequest.Builder withReadConsistency(ReadConsistency readConsistency) {
            this.readConsistency = Preconditions.checkNotNull(readConsistency, "readConsistency cannot be null");
            return this;
        }

        /**
         * Sets the minimum session timeout.
         * @param timeout The minimum session timeout.
         * @return The open session request builder.
         * @throws IllegalArgumentException if {@code timeout} is not positive
         */
        public OpenSessionRequest.Builder withMinTimeout(long timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout must be positive");
            this.minTimeout = timeout;
            return this;
        }

        /**
         * Sets the maximum session timeout.
         * @param timeout The maximum session timeout.
         * @return The open session request builder.
         * @throws IllegalArgumentException if {@code timeout} is not positive
         */
        public OpenSessionRequest.Builder withMaxTimeout(long timeout) {
            Preconditions.checkArgument(timeout >= 0, "timeout must be positive");
            this.maxTimeout = timeout;
            return this;
        }

        /**
         * @throws IllegalStateException is session is not positive
         */
        @Override
        public OpenSessionRequest build() {
            validate();
            return new OpenSessionRequest(memberId, serviceName, serviceType, readConsistency, minTimeout, maxTimeout);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkNotNull(memberId, "client cannot be null");
            Preconditions.checkNotNull(serviceName, "name cannot be null");
            Preconditions.checkNotNull(serviceType, "typeName cannot be null");
            Preconditions.checkArgument(minTimeout >= 0, "minTimeout must be positive");
            Preconditions.checkArgument(maxTimeout >= 0, "maxTimeout must be positive");
        }
    }
}
