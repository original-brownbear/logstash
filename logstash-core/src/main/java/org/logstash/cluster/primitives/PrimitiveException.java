package org.logstash.cluster.primitives;

/**
 * Top level exception for Store failures.
 */
@SuppressWarnings("serial")
public class PrimitiveException extends RuntimeException {
    public PrimitiveException() {
    }

    public PrimitiveException(String message) {
        super(message);
    }

    public PrimitiveException(Throwable t) {
        super(t);
    }

    /**
     * Store is temporarily unavailable.
     */
    public static class Unavailable extends PrimitiveException {
    }

    /**
     * Store operation timeout.
     */
    public static class Timeout extends PrimitiveException {
    }

    /**
     * Store update conflicts with an in flight transaction.
     */
    public static class ConcurrentModification extends PrimitiveException {
    }

    /**
     * Store operation interrupted.
     */
    public static class Interrupted extends PrimitiveException {
    }
}
