package org.logstash.cluster.storage.journal;

import com.google.common.base.MoreObjects;

/**
 * Journal reader delegate.
 */
public class DelegatingJournalReader<E> implements JournalReader<E> {
    private final JournalReader<E> delegate;

    public DelegatingJournalReader(JournalReader<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getCurrentIndex() {
        return delegate.getCurrentIndex();
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        return delegate.getCurrentEntry();
    }

    @Override
    public long getNextIndex() {
        return delegate.getNextIndex();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Indexed<E> next() {
        return delegate.next();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void reset(long index) {
        delegate.reset(index);
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
