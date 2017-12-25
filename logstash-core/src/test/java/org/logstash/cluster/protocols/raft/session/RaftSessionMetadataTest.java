package org.logstash.cluster.protocols.raft.session;

import org.junit.Test;
import org.logstash.cluster.protocols.raft.service.ServiceType;

import static org.junit.Assert.assertEquals;

/**
 * Raft session metadata test.
 */
public class RaftSessionMetadataTest {
    @Test
    public void testRaftSessionMetadata() {
        RaftSessionMetadata metadata = new RaftSessionMetadata(1, "foo", "test");
        assertEquals(SessionId.from(1), metadata.sessionId());
        assertEquals("foo", metadata.serviceName());
        assertEquals(ServiceType.from("test"), metadata.serviceType());
    }
}
