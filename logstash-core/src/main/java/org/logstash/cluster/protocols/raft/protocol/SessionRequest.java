package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * Base session request.
 * <p>
 * This is the base request for session-related requests. Many client requests are handled within the
 * context of a {@link #session()} identifier.
 */
public abstract class SessionRequest extends AbstractRaftRequest {
    protected final long session;

    protected SessionRequest(long session) {
        this.session = session;
    }

    /**
     * Returns the session ID.
     * @return The session ID.
     */
    public long session() {
        return session;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), session);
    }

    @Override
    public boolean equals(Object object) {
        if (object.getClass() == getClass()) {
            SessionRequest request = (SessionRequest) object;
            return request.session == session;
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("session", session)
            .toString();
    }

    /**
     * Session request builder.
     */
    public abstract static class Builder<T extends SessionRequest.Builder<T, U>, U extends SessionRequest> extends AbstractRaftRequest.Builder<T, U> {
        protected long session;

        /**
         * Sets the session ID.
         * @param session The session ID.
         * @return The request builder.
         * @throws IllegalArgumentException if {@code session} is less than 0
         */
        @SuppressWarnings("unchecked")
        public T withSession(long session) {
            Preconditions.checkArgument(session > 0, "session must be positive");
            this.session = session;
            return (T) this;
        }

        @Override
        protected void validate() {
            Preconditions.checkArgument(session > 0, "session must be positive");
        }
    }
}
