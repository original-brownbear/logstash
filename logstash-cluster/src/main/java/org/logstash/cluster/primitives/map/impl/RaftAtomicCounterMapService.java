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
package org.logstash.cluster.primitives.map.impl;

import java.util.HashMap;
import java.util.Map;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.ADD_AND_GET;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.CLEAR;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.DECREMENT_AND_GET;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.GET;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.GET_AND_ADD;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.GET_AND_DECREMENT;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.GET_AND_INCREMENT;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.INCREMENT_AND_GET;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.IS_EMPTY;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.PUT;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.PUT_IF_ABSENT;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.REMOVE;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.REMOVE_VALUE;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.REPLACE;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.SIZE;

/**
 * <p>
 * The counter map state is implemented as a snapshottable state machine. Snapshots are necessary
 * since incremental compaction is impractical for counters where the value of a counter is the sum
 * of all its increments. Note that this snapshotting large state machines may risk blocking of the
 * Raft cluster with the current implementation of snapshotting in Copycat.
 */
public class RaftAtomicCounterMapService extends AbstractRaftService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicCounterMapOperations.NAMESPACE)
        .build());

    private Map<String, Long> map = new HashMap<>();

    @Override
    protected void configure(RaftServiceExecutor executor) {
        executor.register(PUT, SERIALIZER::decode, this::put, SERIALIZER::encode);
        executor.register(PUT_IF_ABSENT, SERIALIZER::decode, this::putIfAbsent, SERIALIZER::encode);
        executor.register(GET, SERIALIZER::decode, this::get, SERIALIZER::encode);
        executor.register(REPLACE, SERIALIZER::decode, this::replace, SERIALIZER::encode);
        executor.register(REMOVE, SERIALIZER::decode, this::remove, SERIALIZER::encode);
        executor.register(REMOVE_VALUE, SERIALIZER::decode, this::removeValue, SERIALIZER::encode);
        executor.register(GET_AND_INCREMENT, SERIALIZER::decode, this::getAndIncrement, SERIALIZER::encode);
        executor.register(GET_AND_DECREMENT, SERIALIZER::decode, this::getAndDecrement, SERIALIZER::encode);
        executor.register(INCREMENT_AND_GET, SERIALIZER::decode, this::incrementAndGet, SERIALIZER::encode);
        executor.register(DECREMENT_AND_GET, SERIALIZER::decode, this::decrementAndGet, SERIALIZER::encode);
        executor.register(ADD_AND_GET, SERIALIZER::decode, this::addAndGet, SERIALIZER::encode);
        executor.register(GET_AND_ADD, SERIALIZER::decode, this::getAndAdd, SERIALIZER::encode);
        executor.register(SIZE, this::size, SERIALIZER::encode);
        executor.register(IS_EMPTY, this::isEmpty, SERIALIZER::encode);
        executor.register(CLEAR, this::clear);
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
        writer.writeObject(map, SERIALIZER::encode);
    }

    @Override
    public void install(SnapshotReader reader) {
        map = reader.readObject(SERIALIZER::decode);
    }

    /**
     * Returns the primitive value for the given primitive wrapper.
     */
    private long primitive(Long value) {
        if (value != null) {
            return value;
        } else {
            return 0;
        }
    }

    /**
     * Handles a {@link Put} command which implements {@link RaftAtomicCounterMap#put(String, long)}.
     * @param commit put commit
     * @return put result
     */
    protected long put(Commit<RaftAtomicCounterMapOperations.Put> commit) {
        return primitive(map.put(commit.value().key(), commit.value().value()));
    }

    /**
     * Handles a {@link PutIfAbsent} command which implements {@link RaftAtomicCounterMap#putIfAbsent(String, long)}.
     * @param commit putIfAbsent commit
     * @return putIfAbsent result
     */
    protected long putIfAbsent(Commit<RaftAtomicCounterMapOperations.PutIfAbsent> commit) {
        return primitive(map.putIfAbsent(commit.value().key(), commit.value().value()));
    }

    /**
     * Handles a {@link Get} query which implements {@link RaftAtomicCounterMap#get(String)}}.
     * @param commit get commit
     * @return get result
     */
    protected long get(Commit<RaftAtomicCounterMapOperations.Get> commit) {
        return primitive(map.get(commit.value().key()));
    }

    /**
     * Handles a {@link Replace} command which implements {@link RaftAtomicCounterMap#replace(String, long, long)}.
     * @param commit replace commit
     * @return replace result
     */
    protected boolean replace(Commit<RaftAtomicCounterMapOperations.Replace> commit) {
        Long value = map.get(commit.value().key());
        if (value == null) {
            if (commit.value().replace() == 0) {
                map.put(commit.value().key(), commit.value().value());
                return true;
            } else {
                return false;
            }
        } else if (value == commit.value().replace()) {
            map.put(commit.value().key(), commit.value().value());
            return true;
        }
        return false;
    }

    /**
     * Handles a {@link Remove} command which implements {@link RaftAtomicCounterMap#remove(String)}.
     * @param commit remove commit
     * @return remove result
     */
    protected long remove(Commit<RaftAtomicCounterMapOperations.Remove> commit) {
        return primitive(map.remove(commit.value().key()));
    }

    /**
     * Handles a {@link RemoveValue} command which implements {@link RaftAtomicCounterMap#remove(String, long)}.
     * @param commit removeValue commit
     * @return removeValue result
     */
    protected boolean removeValue(Commit<RaftAtomicCounterMapOperations.RemoveValue> commit) {
        Long value = map.get(commit.value().key());
        if (value == null) {
            if (commit.value().value() == 0) {
                map.remove(commit.value().key());
                return true;
            }
            return false;
        } else if (value == commit.value().value()) {
            map.remove(commit.value().key());
            return true;
        }
        return false;
    }

    /**
     * Handles a {@link GetAndIncrement} command which implements
     * {@link RaftAtomicCounterMap#getAndIncrement(String)}.
     * @param commit getAndIncrement commit
     * @return getAndIncrement result
     */
    protected long getAndIncrement(Commit<RaftAtomicCounterMapOperations.GetAndIncrement> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value + 1);
        return value;
    }

    /**
     * Handles a {@link GetAndDecrement} command which implements
     * {@link RaftAtomicCounterMap#getAndDecrement(String)}.
     * @param commit getAndDecrement commit
     * @return getAndDecrement result
     */
    protected long getAndDecrement(Commit<RaftAtomicCounterMapOperations.GetAndDecrement> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value - 1);
        return value;
    }

    /**
     * Handles a {@link IncrementAndGet} command which implements
     * {@link RaftAtomicCounterMap#incrementAndGet(String)}.
     * @param commit incrementAndGet commit
     * @return incrementAndGet result
     */
    protected long incrementAndGet(Commit<RaftAtomicCounterMapOperations.IncrementAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), ++value);
        return value;
    }

    /**
     * Handles a {@link DecrementAndGet} command which implements
     * {@link RaftAtomicCounterMap#decrementAndGet(String)}.
     * @param commit decrementAndGet commit
     * @return decrementAndGet result
     */
    protected long decrementAndGet(Commit<RaftAtomicCounterMapOperations.DecrementAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), --value);
        return value;
    }

    /**
     * Handles a {@link AddAndGet} command which implements {@link RaftAtomicCounterMap#addAndGet(String, long)}.
     * @param commit addAndGet commit
     * @return addAndGet result
     */
    protected long addAndGet(Commit<RaftAtomicCounterMapOperations.AddAndGet> commit) {
        long value = primitive(map.get(commit.value().key()));
        value += commit.value().delta();
        map.put(commit.value().key(), value);
        return value;
    }

    /**
     * Handles a {@link GetAndAdd} command which implements {@link RaftAtomicCounterMap#getAndAdd(String, long)}.
     * @param commit getAndAdd commit
     * @return getAndAdd result
     */
    protected long getAndAdd(Commit<RaftAtomicCounterMapOperations.GetAndAdd> commit) {
        long value = primitive(map.get(commit.value().key()));
        map.put(commit.value().key(), value + commit.value().delta());
        return value;
    }

    /**
     * Handles a {@code Size} query which implements {@link RaftAtomicCounterMap#size()}.
     * @param commit size commit
     * @return size result
     */
    protected int size(Commit<Void> commit) {
        return map.size();
    }

    /**
     * Handles an {@code IsEmpty} query which implements {@link RaftAtomicCounterMap#isEmpty()}.
     * @param commit isEmpty commit
     * @return isEmpty result
     */
    protected boolean isEmpty(Commit<Void> commit) {
        return map.isEmpty();
    }

    /**
     * Handles a {@code Clear} command which implements {@link RaftAtomicCounterMap#clear()}.
     * @param commit clear commit
     */
    protected void clear(Commit<Void> commit) {
        map.clear();
    }
}
