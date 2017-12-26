package org.logstash.cluster.protocols.raft.protocol;

/**
 * Base interface for requests.
 */
public interface RaftRequest extends RaftMessage {

    /**
     * Request builder.
     * @param <T> The builder type.
     */
    interface Builder<T extends RaftRequest.Builder<T, U>, U extends RaftRequest> extends org.logstash.cluster.utils.Builder<U> {
    }
}
