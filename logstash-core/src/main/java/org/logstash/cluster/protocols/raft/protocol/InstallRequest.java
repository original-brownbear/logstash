package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Server snapshot installation request.
 * <p>
 * Snapshot installation requests are sent by the leader to a follower when the follower indicates
 * that its log is further behind than the last snapshot taken by the leader. Snapshots are sent
 * in chunks, with each chunk being sent in a separate install request. As requests are received by
 * the follower, the snapshot is reconstructed based on the provided {@link #chunkOffset()} and other
 * metadata. The last install request will be sent with {@link #complete()} being {@code true} to
 * indicate that all chunks of the snapshot have been sent.
 */
public class InstallRequest extends AbstractRaftRequest {

    private final long term;
    private final MemberId leader;
    private final long serviceId;
    private final String serviceName;
    private final long index;
    private final long timestamp;
    private final int offset;
    private final byte[] data;
    private final boolean complete;

    public InstallRequest(long term, MemberId leader, long serviceId, String serviceName, long index, long timestamp, int offset, byte[] data, boolean complete) {
        this.term = term;
        this.leader = leader;
        this.serviceId = serviceId;
        this.serviceName = serviceName;
        this.index = index;
        this.timestamp = timestamp;
        this.offset = offset;
        this.data = data;
        this.complete = complete;
    }

    /**
     * Returns a new install request builder.
     * @return A new install request builder.
     */
    public static InstallRequest.Builder builder() {
        return new InstallRequest.Builder();
    }

    /**
     * Returns the requesting node's current term.
     * @return The requesting node's current term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns the requesting leader address.
     * @return The leader's address.
     */
    public MemberId leader() {
        return leader;
    }

    /**
     * Returns the snapshot identifier.
     * @return The snapshot identifier.
     */
    public long serviceId() {
        return serviceId;
    }

    /**
     * Returns the service name.
     * @return The service name.
     */
    public String serviceName() {
        return serviceName;
    }

    /**
     * Returns the snapshot index.
     * @return The snapshot index.
     */
    public long snapshotIndex() {
        return index;
    }

    /**
     * Returns the snapshot timestamp.
     * @return The snapshot timestamp.
     */
    public long snapshotTimestamp() {
        return timestamp;
    }

    /**
     * Returns the offset of the snapshot chunk.
     * @return The offset of the snapshot chunk.
     */
    public int chunkOffset() {
        return offset;
    }

    /**
     * Returns the snapshot data.
     * @return The snapshot data.
     */
    public byte[] data() {
        return data;
    }

    /**
     * Returns a boolean value indicating whether this is the last chunk of the snapshot.
     * @return Indicates whether this request is the last chunk of the snapshot.
     */
    public boolean complete() {
        return complete;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), term, leader, serviceId, index, offset, complete, data);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof InstallRequest) {
            InstallRequest request = (InstallRequest) object;
            return request.term == term
                && request.leader == leader
                && request.serviceId == serviceId
                && Objects.equals(request.serviceName, serviceName)
                && request.index == index
                && request.offset == offset
                && request.complete == complete
                && Arrays.equals(request.data, data);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("leader", leader)
            .add("id", serviceId)
            .add("name", serviceName)
            .add("index", index)
            .add("offset", offset)
            .add("data", ArraySizeHashPrinter.of(data))
            .add("complete", complete)
            .toString();
    }

    /**
     * Snapshot request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<InstallRequest.Builder, InstallRequest> {
        private long term;
        private MemberId leader;
        private long serviceId;
        private String serviceName;
        private long index;
        private long timestamp;
        private int offset;
        private byte[] data;
        private boolean complete;

        /**
         * Sets the request term.
         * @param term The request term.
         * @return The append request builder.
         * @throws IllegalArgumentException if the {@code term} is not positive
         */
        public InstallRequest.Builder withTerm(long term) {
            Preconditions.checkArgument(term > 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets the request leader.
         * @param leader The request leader.
         * @return The append request builder.
         * @throws IllegalArgumentException if the {@code leader} is not positive
         */
        public InstallRequest.Builder withLeader(MemberId leader) {
            this.leader = Preconditions.checkNotNull(leader, "leader cannot be null");
            return this;
        }

        /**
         * Sets the request snapshot identifier.
         * @param serviceId The request snapshot identifier.
         * @return The request builder.
         */
        public InstallRequest.Builder withServiceId(long serviceId) {
            Preconditions.checkArgument(serviceId > 0, "serviceId must be positive");
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Sets the request service name.
         * @param serviceName The snapshot's service name.
         * @return The request builder.
         */
        public InstallRequest.Builder withServiceName(String serviceName) {
            this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName cannot be null");
            return this;
        }

        /**
         * Sets the request index.
         * @param index The request index.
         * @return The request builder.
         */
        public InstallRequest.Builder withIndex(long index) {
            Preconditions.checkArgument(index >= 0, "index must be positive");
            this.index = index;
            return this;
        }

        /**
         * Sets the request timestamp.
         * @param timestamp The request timestamp.
         * @return The request builder.
         */
        public InstallRequest.Builder withTimestamp(long timestamp) {
            Preconditions.checkArgument(timestamp >= 0, "timestamp must be positive");
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Sets the request offset.
         * @param offset The request offset.
         * @return The request builder.
         */
        public InstallRequest.Builder withOffset(int offset) {
            Preconditions.checkArgument(offset >= 0, "offset must be positive");
            this.offset = offset;
            return this;
        }

        /**
         * Sets the request snapshot bytes.
         * @param data The snapshot bytes.
         * @return The request builder.
         */
        public InstallRequest.Builder withData(byte[] data) {
            this.data = Preconditions.checkNotNull(data, "data cannot be null");
            return this;
        }

        /**
         * Sets whether the request is complete.
         * @param complete Whether the snapshot is complete.
         * @return The request builder.
         * @throws NullPointerException if {@code member} is null
         */
        public InstallRequest.Builder withComplete(boolean complete) {
            this.complete = complete;
            return this;
        }

        /**
         * @throws IllegalStateException if member is null
         */
        @Override
        public InstallRequest build() {
            validate();
            return new InstallRequest(term, leader, serviceId, serviceName, index, timestamp, offset, data, complete);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkArgument(term > 0, "term must be positive");
            Preconditions.checkNotNull(leader, "leader cannot be null");
            Preconditions.checkArgument(serviceId > 0, "serviceId must be positive");
            Preconditions.checkNotNull(serviceName, "serviceName cannot be null");
            Preconditions.checkArgument(index >= 0, "index must be positive");
            Preconditions.checkArgument(offset >= 0, "offset must be positive");
            Preconditions.checkNotNull(data, "data cannot be null");
        }
    }

}
