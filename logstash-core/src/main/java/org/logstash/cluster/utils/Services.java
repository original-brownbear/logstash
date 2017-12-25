package org.logstash.cluster.utils;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Service utilities.
 */
public final class Services {
    private static final Map<Class<?>, Object> SERVICES = Maps.newConcurrentMap();

    private Services() {
    }

    /**
     * Loads the service for the given service class.
     * @param serviceClass the service class for which to load the service
     * @param <T> the service type
     * @return the registered service of the given type
     */
    @SuppressWarnings("unchecked")
    public static <T> T load(final Class<T> serviceClass) {
        return (T) SERVICES.computeIfAbsent(serviceClass, s -> ServiceLoader.load(serviceClass).iterator().next());
    }

}
