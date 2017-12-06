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
package org.logstash.cluster.primitives.impl;

import java.util.Map;
import java.util.Set;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.PrimitiveService;
import org.logstash.cluster.primitives.counter.AtomicCounterBuilder;
import org.logstash.cluster.primitives.counter.impl.DefaultAtomicCounterBuilder;
import org.logstash.cluster.primitives.generator.AtomicIdGeneratorBuilder;
import org.logstash.cluster.primitives.generator.impl.DefaultAtomicIdGeneratorBuilder;
import org.logstash.cluster.primitives.leadership.LeaderElectorBuilder;
import org.logstash.cluster.primitives.leadership.impl.DefaultLeaderElectorBuilder;
import org.logstash.cluster.primitives.lock.DistributedLockBuilder;
import org.logstash.cluster.primitives.lock.impl.DefaultDistributedLockBuilder;
import org.logstash.cluster.primitives.map.AtomicCounterMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentTreeMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultAtomicCounterMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultConsistentMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultConsistentTreeMapBuilder;
import org.logstash.cluster.primitives.multimap.ConsistentMultimapBuilder;
import org.logstash.cluster.primitives.multimap.impl.DefaultConsistentMultimapBuilder;
import org.logstash.cluster.primitives.queue.WorkQueueBuilder;
import org.logstash.cluster.primitives.queue.impl.DefaultWorkQueueBuilder;
import org.logstash.cluster.primitives.set.DistributedSetBuilder;
import org.logstash.cluster.primitives.set.impl.DefaultDistributedSetBuilder;
import org.logstash.cluster.primitives.tree.DocumentTreeBuilder;
import org.logstash.cluster.primitives.tree.impl.DefaultDocumentTreeBuilder;
import org.logstash.cluster.primitives.value.AtomicValueBuilder;
import org.logstash.cluster.primitives.value.impl.DefaultAtomicValueBuilder;

/**
 * Partitioned primitive service.
 */
public class FederatedPrimitiveService implements PrimitiveService {
    private final DistributedPrimitiveCreator federatedPrimitiveCreator;

    public FederatedPrimitiveService(Map<Integer, DistributedPrimitiveCreator> members, int buckets) {
        this.federatedPrimitiveCreator = new FederatedDistributedPrimitiveCreator(members, buckets);
    }

    @Override
    public <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder() {
        return new DefaultConsistentMapBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <V> DocumentTreeBuilder<V> documentTreeBuilder() {
        return new DefaultDocumentTreeBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder() {
        return new DefaultConsistentTreeMapBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder() {
        return new DefaultConsistentMultimapBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder() {
        return new DefaultAtomicCounterMapBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <E> DistributedSetBuilder<E> setBuilder() {
        return new DefaultDistributedSetBuilder<>(() -> consistentMapBuilder());
    }

    @Override
    public AtomicCounterBuilder atomicCounterBuilder() {
        return new DefaultAtomicCounterBuilder(federatedPrimitiveCreator);
    }

    @Override
    public AtomicIdGeneratorBuilder atomicIdGeneratorBuilder() {
        return new DefaultAtomicIdGeneratorBuilder(federatedPrimitiveCreator);
    }

    @Override
    public <V> AtomicValueBuilder<V> atomicValueBuilder() {
        return new DefaultAtomicValueBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public <T> LeaderElectorBuilder<T> leaderElectorBuilder() {
        return new DefaultLeaderElectorBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public DistributedLockBuilder lockBuilder() {
        return new DefaultDistributedLockBuilder(federatedPrimitiveCreator);
    }

    @Override
    public <E> WorkQueueBuilder<E> workQueueBuilder() {
        return new DefaultWorkQueueBuilder<>(federatedPrimitiveCreator);
    }

    @Override
    public Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType) {
        return federatedPrimitiveCreator.getPrimitiveNames(primitiveType);
    }
}
