package org.logstash.cluster.protocols.bootstrap;

import org.logstash.cluster.event.ListenerService;

/**
 * Primary bootstrap service.
 */
public interface BootstrapService<T> extends ListenerService<BootstrapEvent<T>, BootstrapEventListener<T>> {

    /**
     * Closes the service.
     */
    void close();

    /**
     * Bootstrap service builder.
     * @param <T> the value type
     */
    interface Builder<T> extends org.logstash.cluster.utils.Builder<BootstrapService<T>> {
    }
}
