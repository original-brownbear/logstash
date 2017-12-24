package org.logstash.cluster.primitives.map;

import org.logstash.cluster.primitives.PrimitiveException;

/**
 * Top level exception for ConsistentMap failures.
 */
@SuppressWarnings("serial")
public class ConsistentMapException extends PrimitiveException {
    public ConsistentMapException() {
    }

    public ConsistentMapException(String message) {
        super(message);
    }

    public ConsistentMapException(Throwable t) {
        super(t);
    }

    /**
     * ConsistentMap operation timeout.
     */
    public static class Timeout extends ConsistentMapException {
        public Timeout() {
            super();
        }

        public Timeout(String message) {
            super(message);
        }
    }

    /**
     * ConsistentMap update conflicts with an in flight transaction.
     */
    public static class ConcurrentModification extends ConsistentMapException {
        public ConcurrentModification() {
            super();
        }

        public ConcurrentModification(String message) {
            super(message);
        }
    }

    /**
     * ConsistentMap operation interrupted.
     */
    public static class Interrupted extends ConsistentMapException {
    }
}
