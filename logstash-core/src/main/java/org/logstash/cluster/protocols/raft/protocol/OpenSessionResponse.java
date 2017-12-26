package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Open session response.
 */
public class OpenSessionResponse extends AbstractRaftResponse {

    protected final long session;
    protected final long timeout;

    public OpenSessionResponse(RaftResponse.Status status, RaftError error, long session, long timeout) {
        super(status, error);
        this.session = session;
        this.timeout = timeout;
    }

    /**
     * Returns a new register client response builder.
     * @return A new register client response builder.
     */
    public static OpenSessionResponse.Builder builder() {
        return new OpenSessionResponse.Builder();
    }

    /**
     * Returns the registered session ID.
     * @return The registered session ID.
     */
    public long session() {
        return session;
    }

    /**
     * Returns the session timeout.
     * @return The session timeout.
     */
    public long timeout() {
        return timeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), error, status, session, timeout);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof OpenSessionResponse) {
            OpenSessionResponse response = (OpenSessionResponse) object;
            return response.status == status
                && Objects.equals(response.error, error)
                && response.session == session
                && response.timeout == timeout;
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == RaftResponse.Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("session", session)
                .add("timeout", timeout)
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Register response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<OpenSessionResponse.Builder, OpenSessionResponse> {
        private long session;
        private long timeout;

        /**
         * Sets the response session ID.
         * @param session The session ID.
         * @return The register response builder.
         * @throws IllegalArgumentException if {@code session} is less than 1
         */
        public OpenSessionResponse.Builder withSession(long session) {
            Preconditions.checkArgument(session > 0, "session must be positive");
            this.session = session;
            return this;
        }

        /**
         * Sets the session timeout.
         * @param timeout The session timeout.
         * @return The response builder.
         */
        public OpenSessionResponse.Builder withTimeout(long timeout) {
            Preconditions.checkArgument(timeout > 0, "timeout must be positive");
            this.timeout = timeout;
            return this;
        }

        @Override
        public OpenSessionResponse build() {
            validate();
            return new OpenSessionResponse(status, error, session, timeout);
        }

        @Override
        protected void validate() {
            super.validate();
            if (status == RaftResponse.Status.OK) {
                Preconditions.checkArgument(session > 0, "session must be positive");
                Preconditions.checkArgument(timeout > 0, "timeout must be positive");
            }
        }
    }
}
