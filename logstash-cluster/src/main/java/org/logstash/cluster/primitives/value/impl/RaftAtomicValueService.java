/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.primitives.value.impl;

import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.logstash.cluster.primitives.value.AtomicValueEvent;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueEvents.CHANGE;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.ADD_LISTENER;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.COMPARE_AND_SET;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.GET;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.GET_AND_SET;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.REMOVE_LISTENER;
import static org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations.SET;

/**
 * Raft atomic value service.
 */
public class RaftAtomicValueService extends AbstractRaftService {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicValueOperations.NAMESPACE)
        .register(RaftAtomicValueEvents.NAMESPACE)
        .build());

    private byte[] value = new byte[0];
    private Set<RaftSession> listeners = Sets.newHashSet();

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(SET, SERIALIZER::decode, this::set);
        executor.register(GET, this::get, SERIALIZER::encode);
        executor.register(COMPARE_AND_SET, SERIALIZER::decode, this::compareAndSet, SERIALIZER::encode);
        executor.register(GET_AND_SET, SERIALIZER::decode, this::getAndSet, SERIALIZER::encode);
        executor.register(ADD_LISTENER, (Commit<Void> c) -> listeners.add(c.session()));
        executor.register(REMOVE_LISTENER, (Commit<Void> c) -> listeners.remove(c.session()));
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeInt(value.length).writeBytes(value);
        Set<Long> sessionIds = new HashSet<>();
        for (RaftSession session : listeners) {
            sessionIds.add(session.sessionId().id());
        }
        writer.writeObject(sessionIds, SERIALIZER::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        value = reader.readBytes(reader.readInt());
        listeners = new HashSet<>();
        for (Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
            listeners.add(sessions().getSession(sessionId));
        }
    }

    /**
     * Handles a set commit.
     * @param commit the commit to handle
     */
    protected void set(Commit<RaftAtomicValueOperations.Set> commit) {
        if (!Arrays.equals(this.value, commit.value().value())) {
            updateAndNotify(commit.value().value());
        }
    }

    private byte[] updateAndNotify(byte[] value) {
        byte[] oldValue = this.value;
        this.value = value;
        AtomicValueEvent<byte[]> event = new AtomicValueEvent<>(oldValue, value);
        listeners.forEach(s -> s.publish(CHANGE, SERIALIZER::encode, event));
        return oldValue;
    }

    /**
     * Handles a get commit.
     * @param commit the commit to handle
     * @return value
     */
    protected byte[] get(Commit<Void> commit) {
        return value;
    }

    /**
     * Handles a compare and set commit.
     * @param commit the commit to handle
     * @return indicates whether the value was updated
     */
    protected boolean compareAndSet(Commit<RaftAtomicValueOperations.CompareAndSet> commit) {
        if (Arrays.equals(value, commit.value().expect())) {
            updateAndNotify(commit.value().update());
            return true;
        }
        return false;
    }

    /**
     * Handles a get and set commit.
     * @param commit the commit to handle
     * @return value
     */
    protected byte[] getAndSet(Commit<RaftAtomicValueOperations.GetAndSet> commit) {
        return updateAndNotify(commit.value().value());
    }
}
