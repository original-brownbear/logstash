package org.logstash.cluster.storage.journal;

import com.google.common.base.MoreObjects;

/**
 * Journal writer delegate.
 */
public class DelegatingJournalWriter<E> implements JournalWriter<E> {
    private final JournalWriter<E> delegate;

    public DelegatingJournalWriter(JournalWriter<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getLastIndex() {
        return delegate.getLastIndex();
    }

    @Override
    public Indexed<E> getLastEntry() {
        return delegate.getLastEntry();
    }

    @Override
    public long getNextIndex() {
        return delegate.getNextIndex();
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        return delegate.append(entry);
    }

    @Override
    public void append(Indexed<E> entry) {
        delegate.append(entry);
    }

    @Override
    public void truncate(long index) {
        delegate.truncate(index);
    }

    @Override
    public void flush() {
        delegate.flush();
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
