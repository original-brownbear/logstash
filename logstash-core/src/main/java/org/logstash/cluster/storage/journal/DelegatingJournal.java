package org.logstash.cluster.storage.journal;

import com.google.common.base.MoreObjects;

/**
 * Delegating journal.
 */
public class DelegatingJournal<E> implements Journal<E> {
    private final Journal<E> delegate;

    public DelegatingJournal(Journal<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public JournalWriter<E> writer() {
        return delegate.writer();
    }

    @Override
    public JournalReader<E> openReader(long index) {
        return delegate.openReader(index);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("delegate", delegate)
            .toString();
    }
}
