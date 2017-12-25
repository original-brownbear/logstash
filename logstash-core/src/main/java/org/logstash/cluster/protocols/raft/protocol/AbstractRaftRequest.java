package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;

/**
 * Base request for all client requests.
 */
public abstract class AbstractRaftRequest implements RaftRequest {

    /**
     * Abstract request builder.
     * @param <T> The builder type.
     * @param <U> The request type.
     */
    protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRaftRequest> implements RaftRequest.Builder<T, U> {

        /**
         * Validates the builder.
         */
        protected void validate() {
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).toString();
        }
    }
}
