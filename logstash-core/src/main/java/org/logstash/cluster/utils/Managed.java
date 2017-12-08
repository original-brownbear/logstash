package org.logstash.cluster.utils;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for types that can be asynchronously opened and closed.
 * @param <T> managed type
 */
public interface Managed<T> {

    /**
     * Opens the managed object.
     * @return A completable future to be completed once the object has been opened.
     */
    CompletableFuture<T> open();

    /**
     * Returns a boolean value indicating whether the managed object is open.
     * @return Indicates whether the managed object is open.
     */
    boolean isOpen();

    /**
     * Closes the managed object.
     * @return A completable future to be completed once the object has been closed.
     */
    CompletableFuture<Void> close();

    /**
     * Returns a boolean value indicating whether the managed object is closed.
     * @return Indicates whether the managed object is closed.
     */
    boolean isClosed();

}
