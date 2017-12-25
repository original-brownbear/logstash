package org.logstash.cluster.protocols.raft.session;

import org.logstash.cluster.event.EventListener;

/**
 * Raft session event listener.
 */
@FunctionalInterface
public interface RaftSessionEventListener extends EventListener<RaftSessionEvent> {
}
