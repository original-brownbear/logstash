package org.logstash.cluster.storage.journal;

import java.io.Closeable;

/**
 * Journal.
 */
public interface Journal<E> extends Closeable {

    /**
     * Returns the journal writer.
     * @return The journal writer.
     */
    JournalWriter<E> writer();

    /**
     * Opens a new journal reader.
     * @param index The index at which to start the reader.
     * @return A new journal reader.
     */
    JournalReader<E> openReader(long index);

    /**
     * Returns a boolean indicating whether the journal is open.
     * @return Indicates whether the journal is open.
     */
    boolean isOpen();

    @Override
    void close();
}
