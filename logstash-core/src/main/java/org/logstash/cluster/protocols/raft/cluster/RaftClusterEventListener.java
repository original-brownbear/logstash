package org.logstash.cluster.protocols.raft.cluster;

import org.logstash.cluster.event.EventListener;

/**
 * Raft cluster event listener.
 */
@FunctionalInterface
public interface RaftClusterEventListener extends EventListener<RaftClusterEvent> {
}
