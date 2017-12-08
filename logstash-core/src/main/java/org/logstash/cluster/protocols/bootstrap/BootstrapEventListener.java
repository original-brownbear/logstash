package org.logstash.cluster.protocols.bootstrap;

import org.logstash.cluster.event.EventListener;

/**
 * Bootstrap event listener.
 */
public interface BootstrapEventListener<T> extends EventListener<BootstrapEvent<T>> {
}
