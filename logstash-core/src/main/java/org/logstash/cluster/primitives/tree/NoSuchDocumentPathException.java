package org.logstash.cluster.primitives.tree;

/**
 * An exception to be thrown when an invalid path is passed to the
 * {@code DocumentTree}.
 */
public class NoSuchDocumentPathException extends DocumentException {
    public NoSuchDocumentPathException() {
    }

    public NoSuchDocumentPathException(String message) {
        super(message);
    }

    public NoSuchDocumentPathException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchDocumentPathException(Throwable cause) {
        super(cause);
    }
}
