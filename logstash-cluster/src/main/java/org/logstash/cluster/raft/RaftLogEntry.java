package org.logstash.cluster.raft;

import java.io.Serializable;

@FunctionalInterface
public interface RaftLogEntry extends Serializable {

    void applyTo(RaftStateMachine state);
}
