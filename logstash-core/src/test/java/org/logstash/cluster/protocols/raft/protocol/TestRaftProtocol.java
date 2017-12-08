/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
