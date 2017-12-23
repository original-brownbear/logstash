package org.logstash.cluster.primitives.tree;

/**
 * An exception thrown when an illegally named node is submitted.
 */
public class IllegalDocumentNameException extends DocumentException {
    public IllegalDocumentNameException() {
    }

    public IllegalDocumentNameException(String message) {
        super(message);
    }

    public IllegalDocumentNameException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalDocumentNameException(Throwable cause) {
        super(cause);
    }
}
