package org.logstash.cluster.protocols.raft.proxy.impl;

import java.util.UUID;
import org.junit.Test;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.SessionId;

import static org.junit.Assert.assertEquals;

/**
 * Client session state test.
 */
public class RaftProxyStateTest {

    /**
     * Tests session state defaults.
     */
    @Test
    public void testSessionStateDefaults() {
        String sessionName = UUID.randomUUID().toString();
        RaftProxyState state = new RaftProxyState("test", SessionId.from(1), sessionName, ServiceType.from("test"), 1000);
        assertEquals(state.getSessionId(), SessionId.from(1));
        assertEquals(state.getServiceName(), sessionName);
        assertEquals(state.getServiceType().id(), "test");
        assertEquals(state.getCommandRequest(), 0);
        assertEquals(state.getCommandResponse(), 0);
        assertEquals(state.getResponseIndex(), 1);
        assertEquals(state.getEventIndex(), 1);
    }

    /**
     * Tests updating client session state.
     */
    @Test
    public void testSessionState() {
        RaftProxyState state = new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), ServiceType.from("test"), 1000);
        assertEquals(state.getSessionId(), SessionId.from(1));
        assertEquals(state.getResponseIndex(), 1);
        assertEquals(state.getEventIndex(), 1);
        state.setCommandRequest(2);
        assertEquals(state.getCommandRequest(), 2);
        assertEquals(state.nextCommandRequest(), 3);
        assertEquals(state.getCommandRequest(), 3);
        state.setCommandResponse(3);
        assertEquals(state.getCommandResponse(), 3);
        state.setResponseIndex(4);
        assertEquals(state.getResponseIndex(), 4);
        state.setResponseIndex(3);
        assertEquals(state.getResponseIndex(), 4);
        state.setEventIndex(5);
        assertEquals(state.getEventIndex(), 5);
    }

}
