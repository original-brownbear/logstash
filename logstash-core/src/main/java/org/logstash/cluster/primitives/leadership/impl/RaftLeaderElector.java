/*
 * Copyright 2016-present Open Networking Foundation
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
package org.logstash.cluster.primitives.leadership.impl;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.primitives.leadership.LeadershipEvent;
import org.logstash.cluster.primitives.leadership.LeadershipEventListener;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;

/**
 * Distributed resource providing the {@link AsyncLeaderElector} primitive.
 */
public class RaftLeaderElector extends AbstractRaftPrimitive implements AsyncLeaderElector<byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(RaftLeaderElectorOperations.NAMESPACE)
        .register(RaftLeaderElectorEvents.NAMESPACE)
        .build());

    private final Set<LeadershipEventListener> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();

    public RaftLeaderElector(RaftProxy proxy) {
        super(proxy);
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isListening()) {
                proxy.invoke(RaftLeaderElectorOperations.ADD_LISTENER);
            }
        });
        proxy.addEventListener(RaftLeaderElectorEvents.CHANGE, SERIALIZER::decode, this::handleEvent);
    }

    private boolean isListening() {
        return !leadershipChangeListeners.isEmpty();
    }

    private void handleEvent(List<LeadershipEvent> changes) {
        changes.forEach(change -> leadershipChangeListeners.forEach(l -> l.onEvent(change)));
    }

    @Override
    public CompletableFuture<Leadership<byte[]>> run(byte[] id) {
        return proxy.invoke(RaftLeaderElectorOperations.RUN, SERIALIZER::encode, new RaftLeaderElectorOperations.Run(id), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> withdraw(byte[] id) {
        return proxy.invoke(RaftLeaderElectorOperations.WITHDRAW, SERIALIZER::encode, new RaftLeaderElectorOperations.Withdraw(id));
    }

    @Override
    public CompletableFuture<Boolean> anoint(byte[] id) {
        return proxy.<RaftLeaderElectorOperations.Anoint, Boolean>invoke(RaftLeaderElectorOperations.ANOINT, SERIALIZER::encode, new RaftLeaderElectorOperations.Anoint(id), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> evict(byte[] id) {
        return proxy.invoke(RaftLeaderElectorOperations.EVICT, SERIALIZER::encode, new RaftLeaderElectorOperations.Evict(id));
    }

    @Override
    public CompletableFuture<Boolean> promote(byte[] id) {
        return proxy.<RaftLeaderElectorOperations.Promote, Boolean>invoke(RaftLeaderElectorOperations.PROMOTE, SERIALIZER::encode, new RaftLeaderElectorOperations.Promote(id), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Leadership<byte[]>> getLeadership() {
        return proxy.invoke(RaftLeaderElectorOperations.GET_LEADERSHIP, SERIALIZER::decode);
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(LeadershipEventListener listener) {
        if (leadershipChangeListeners.isEmpty()) {
            return proxy.invoke(RaftLeaderElectorOperations.ADD_LISTENER).thenRun(() -> leadershipChangeListeners.add(listener));
        } else {
            leadershipChangeListeners.add(listener);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener listener) {
        if (leadershipChangeListeners.remove(listener) && leadershipChangeListeners.isEmpty()) {
            return proxy.invoke(RaftLeaderElectorOperations.REMOVE_LISTENER).thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }
}
