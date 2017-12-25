package org.logstash.cluster.messaging.impl;

import com.google.common.collect.Maps;
import java.util.Map;
import org.logstash.cluster.messaging.Endpoint;
import org.logstash.cluster.messaging.ManagedMessagingService;

/**
 * Test messaging service factory.
 */
public class TestMessagingServiceFactory {
    private final Map<Endpoint, TestMessagingService> services = Maps.newConcurrentMap();

    /**
     * Returns a new test messaging service for the given endpoint.
     * @param endpoint the endpoint for which to return a messaging service
     * @return the messaging service for the given endpoint
     */
    public ManagedMessagingService newMessagingService(Endpoint endpoint) {
        return new TestMessagingService(endpoint, services);
    }
}
