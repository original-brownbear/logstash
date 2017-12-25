package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.collect.Maps;
import java.util.Map;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Test Raft protocol factory.
 */
public class TestRaftProtocolFactory {
    private final Map<MemberId, TestRaftServerProtocol> servers = Maps.newConcurrentMap();
    private final Map<MemberId, TestRaftClientProtocol> clients = Maps.newConcurrentMap();

    /**
     * Returns a new test client protocol.
     * @param memberId the client member identifier
     * @return a new test client protocol
     */
    public RaftClientProtocol newClientProtocol(MemberId memberId) {
        return new TestRaftClientProtocol(memberId, servers, clients);
    }

    /**
     * Returns a new test server protocol.
     * @param memberId the server member identifier
     * @return a new test server protocol
     */
    public RaftServerProtocol newServerProtocol(MemberId memberId) {
        return new TestRaftServerProtocol(memberId, servers, clients);
    }
}
