package org.logstash.cluster.storage;

/**
 * Log exception.
 */
public class StorageException extends RuntimeException {

    public StorageException() {
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }

    /**
     * Exception thrown when storage runs out of disk space.
     */
    public static class OutOfDiskSpace extends StorageException {
        public OutOfDiskSpace(String message) {
            super(message);
        }
    }
}
