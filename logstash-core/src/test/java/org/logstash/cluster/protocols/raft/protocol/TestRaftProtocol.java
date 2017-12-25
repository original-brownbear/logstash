package org.logstash.cluster.protocols.raft.protocol;

import java.util.Map;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Base class for Raft protocol.
 */
public abstract class TestRaftProtocol {
    private final Map<MemberId, TestRaftServerProtocol> servers;
    private final Map<MemberId, TestRaftClientProtocol> clients;

    public TestRaftProtocol(Map<MemberId, TestRaftServerProtocol> servers, Map<MemberId, TestRaftClientProtocol> clients) {
        this.servers = servers;
        this.clients = clients;
    }

    TestRaftServerProtocol server(MemberId memberId) {
        return servers.get(memberId);
    }

    TestRaftClientProtocol client(MemberId memberId) {
        return clients.get(memberId);
    }
}
