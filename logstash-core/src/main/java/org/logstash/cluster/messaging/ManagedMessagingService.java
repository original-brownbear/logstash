package org.logstash.cluster.messaging;

import org.logstash.cluster.utils.Managed;

/**
 * Managed messaging service.
 */
public interface ManagedMessagingService extends MessagingService, Managed<MessagingService> {
}
