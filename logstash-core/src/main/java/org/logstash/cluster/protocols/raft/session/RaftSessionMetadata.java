package org.logstash.cluster.protocols.raft.session;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.service.ServiceType;

/**
 * Raft session metadata.
 */
public final class RaftSessionMetadata {
    private final long id;
    private final String name;
    private final String type;

    public RaftSessionMetadata(long id, String name, String type) {
        this.id = id;
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
    }

    /**
     * Returns the globally unique session identifier.
     * @return The globally unique session identifier.
     */
    public SessionId sessionId() {
        return SessionId.from(id);
    }

    /**
     * Returns the session name.
     * @return The session name.
     */
    public String serviceName() {
        return name;
    }

    /**
     * Returns the session type.
     * @return The session type.
     */
    public ServiceType serviceType() {
        return ServiceType.from(type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, name);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof RaftSessionMetadata) {
            RaftSessionMetadata metadata = (RaftSessionMetadata) object;
            return metadata.id == id && Objects.equals(metadata.name, name) && Objects.equals(metadata.type, type);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("name", name)
            .add("type", type)
            .toString();
    }
}
