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
package org.logstash.cluster.protocols.gossip;

import org.logstash.cluster.event.EventSink;
import org.logstash.cluster.event.ListenerService;

/**
 * Gossip service.
 */
public interface GossipService<K, V> extends ListenerService<GossipEvent<K, V>, GossipEventListener<K, V>>, EventSink<GossipEvent<K, V>> {

    /**
     * Closes the service.
     */
    void close();

    /**
     * Gossip service builder.
     * @param <K> the gossip subject type
     * @param <V> the gossip value type
     */
    interface Builder<K, V> extends org.logstash.cluster.utils.Builder<GossipService<K, V>> {
    }
}
