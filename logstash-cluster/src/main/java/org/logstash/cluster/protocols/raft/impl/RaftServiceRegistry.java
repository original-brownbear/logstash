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
package org.logstash.cluster.protocols.raft.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.logstash.cluster.protocols.raft.service.impl.DefaultServiceContext;

/**
 * Raft service registry.
 */
public class RaftServiceRegistry implements Iterable<DefaultServiceContext> {
    private final Map<String, DefaultServiceContext> services = new ConcurrentHashMap<>();

    /**
     * Registers a new service.
     * @param service the service to register
     */
    public void registerService(DefaultServiceContext service) {
        services.put(service.serviceName(), service);
    }

    /**
     * Gets a registered service by name.
     * @param name the service name
     * @return the registered service
     */
    public DefaultServiceContext getService(String name) {
        return services.get(name);
    }

    @Override
    public Iterator<DefaultServiceContext> iterator() {
        return services.values().iterator();
    }

    /**
     * Returns a copy of the services registered in the registry.
     * @return a copy of the registered services
     */
    public Collection<DefaultServiceContext> copyValues() {
        return new ArrayList<>(services.values());
    }
}
