package org.logstash.cluster.storage.journal;

import java.util.Iterator;

/**
 * Log reader.
 */
public interface JournalReader<E> extends Iterator<Indexed<E>>, AutoCloseable {

    /**
     * Returns the current reader index.
     * @return The current reader index.
     */
    long getCurrentIndex();

    /**
     * Returns the last read entry.
     * @return The last read entry.
     */
    Indexed<E> getCurrentEntry();

    /**
     * Returns the next reader index.
     * @return The next reader index.
     */
    long getNextIndex();

    /**
     * Returns whether the reader has a next entry to read.
     * @return Whether the reader has a next entry to read.
     */
    @Override
    boolean hasNext();

    /**
     * Returns the next entry in the reader.
     * @return The next entry in the reader.
     */
    @Override
    Indexed<E> next();

    /**
     * Resets the reader to the start.
     */
    void reset();

    /**
     * Resets the reader to the given index.
     * @param index The index to which to reset the reader.
     */
    void reset(long index);

    @Override
    void close();
}
