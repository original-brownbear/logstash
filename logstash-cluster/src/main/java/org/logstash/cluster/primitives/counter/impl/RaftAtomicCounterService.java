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
package org.logstash.cluster.primitives.counter.impl;

import java.util.Objects;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.AddAndGet;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.CompareAndSet;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.GetAndAdd;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.Set;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.ADD_AND_GET;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.COMPARE_AND_SET;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.GET;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.GET_AND_ADD;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.GET_AND_INCREMENT;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.INCREMENT_AND_GET;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.SET;

/**
 * Atomix long state.
 */
public class RaftAtomicCounterService extends AbstractRaftService {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicCounterOperations.NAMESPACE)
        .build());

    private Long value = 0L;

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(SET, SERIALIZER::decode, this::set);
        executor.register(GET, this::get, SERIALIZER::encode);
        executor.register(COMPARE_AND_SET, SERIALIZER::decode, this::compareAndSet, SERIALIZER::encode);
        executor.register(INCREMENT_AND_GET, this::incrementAndGet, SERIALIZER::encode);
        executor.register(GET_AND_INCREMENT, this::getAndIncrement, SERIALIZER::encode);
        executor.register(ADD_AND_GET, SERIALIZER::decode, this::addAndGet, SERIALIZER::encode);
        executor.register(GET_AND_ADD, SERIALIZER::decode, this::getAndAdd, SERIALIZER::encode);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeLong(value);
    }

    @Override
    public void install(SnapshotReader reader) {
        value = reader.readLong();
    }

    /**
     * Handles a set commit.
     * @param commit the commit to handle
     */
    protected void set(Commit<Set> commit) {
        value = commit.value().value();
    }

    /**
     * Handles a get commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected Long get(Commit<Void> commit) {
        return value;
    }

    /**
     * Handles a compare and set commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected boolean compareAndSet(Commit<CompareAndSet> commit) {
        if (Objects.equals(value, commit.value().expect())) {
            value = commit.value().update();
            return true;
        }
        return false;
    }

    /**
     * Handles an increment and get commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected long incrementAndGet(Commit<Void> commit) {
        Long oldValue = value;
        value = oldValue + 1;
        return value;
    }

    /**
     * Handles a get and increment commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected long getAndIncrement(Commit<Void> commit) {
        Long oldValue = value;
        value = oldValue + 1;
        return oldValue;
    }

    /**
     * Handles an add and get commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected long addAndGet(Commit<AddAndGet> commit) {
        Long oldValue = value;
        value = oldValue + commit.value().delta();
        return value;
    }

    /**
     * Handles a get and add commit.
     * @param commit the commit to handle
     * @return counter value
     */
    protected long getAndAdd(Commit<GetAndAdd> commit) {
        Long oldValue = value;
        value = oldValue + commit.value().delta();
        return oldValue;
    }
}
