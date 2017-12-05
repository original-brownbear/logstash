package org.logstash.cluster.raft;

public enum RaftRpcType {

    APPEND_ENTRIES,

    REQUEST_VOTE,

    INSTALL_SNAPSHOT
}
