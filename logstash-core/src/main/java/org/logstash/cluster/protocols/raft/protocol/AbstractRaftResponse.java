package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Base response for all client responses.
 */
public abstract class AbstractRaftResponse implements RaftResponse {
    protected final Status status;
    protected final RaftError error;

    protected AbstractRaftResponse(Status status, RaftError error) {
        this.status = status;
        this.error = error;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public RaftError error() {
        return error;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status);
    }

    @Override
    public boolean equals(Object object) {
        if (object.getClass() == getClass()) {
            AbstractRaftResponse response = (AbstractRaftResponse) object;
            return response.status == status && Objects.equals(response.error, error);
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Abstract response builder.
     * @param <T> The builder type.
     * @param <U> The response type.
     */
    protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRaftResponse> implements RaftResponse.Builder<T, U> {
        protected Status status;
        protected RaftError error;

        @Override
        @SuppressWarnings("unchecked")
        public T withStatus(Status status) {
            this.status = Preconditions.checkNotNull(status, "status cannot be null");
            return (T) this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T withError(RaftError error) {
            this.error = Preconditions.checkNotNull(error, "error cannot be null");
            return (T) this;
        }

        /**
         * Validates the builder.
         */
        protected void validate() {
            Preconditions.checkNotNull(status, "status cannot be null");
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }
}
