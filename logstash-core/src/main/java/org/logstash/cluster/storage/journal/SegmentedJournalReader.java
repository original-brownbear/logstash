package org.logstash.cluster.storage.journal;

import java.util.NoSuchElementException;

/**
 * Segmented journal reader.
 */
public class SegmentedJournalReader<E> implements JournalReader<E> {
    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private Indexed<E> previousEntry;
    private JournalSegmentReader<E> currentReader;

    public SegmentedJournalReader(SegmentedJournal<E> journal, long index) {
        this.journal = journal;
        initialize(index);
    }

    /**
     * Initializes the reader to the given index.
     */
    private void initialize(long index) {
        currentSegment = journal.getSegment(index);
        currentReader = currentSegment.createReader();
        long nextIndex = getNextIndex();
        while (index > nextIndex && hasNext()) {
            next();
            nextIndex = getNextIndex();
        }
    }

    /**
     * Returns the first index in the journal.
     * @return the first index in the journal
     */
    public long getFirstIndex() {
        return journal.getFirstSegment().index();
    }

    @Override
    public long getCurrentIndex() {
        long currentIndex = currentReader.getCurrentIndex();
        if (currentIndex != 0) {
            return currentIndex;
        }
        if (previousEntry != null) {
            return previousEntry.index();
        }
        return 0;
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        Indexed<E> currentEntry = currentReader.getCurrentEntry();
        if (currentEntry != null) {
            return currentEntry;
        }
        return previousEntry;
    }

    @Override
    public long getNextIndex() {
        return currentReader.getNextIndex();
    }

    @Override
    public boolean hasNext() {
        if (!currentReader.hasNext()) {
            JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
            if (nextSegment != null) {
                previousEntry = currentReader.getCurrentEntry();
                currentSegment = nextSegment;
                currentReader = currentSegment.createReader();
                return currentReader.hasNext();
            }
            return false;
        }
        return true;
    }

    @Override
    public Indexed<E> next() {
        if (!currentReader.hasNext()) {
            JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
            if (nextSegment != null) {
                previousEntry = currentReader.getCurrentEntry();
                currentSegment = nextSegment;
                currentReader = currentSegment.createReader();
                return currentReader.next();
            } else {
                throw new NoSuchElementException();
            }
        } else {
            previousEntry = currentReader.getCurrentEntry();
            return currentReader.next();
        }
    }

    @Override
    public void reset() {
        currentReader.close();
        currentSegment = journal.getFirstSegment();
        currentReader = currentSegment.createReader();
        previousEntry = null;
    }

    @Override
    public void reset(long index) {
        // If the current segment is not open, it has been replaced. Reset the segments.
        if (!currentSegment.isOpen()) {
            reset();
        }

        if (index < currentReader.getNextIndex()) {
            rewind(index);
        } else if (index > currentReader.getNextIndex()) {
            forward(index);
        }
    }

    /**
     * Rewinds the journal to the given index.
     */
    private void rewind(long index) {
        if (currentSegment.index() >= index) {
            JournalSegment<E> segment = journal.getSegment(index - 1);
            if (segment != null) {
                currentReader.close();
                currentSegment = segment;
                currentReader = currentSegment.createReader();
            }
        }

        currentReader.reset(index);
        previousEntry = currentReader.getCurrentEntry();
    }

    /**
     * Fast forwards the journal to the given index.
     */
    private void forward(long index) {
        while (getNextIndex() < index && hasNext()) {
            next();
        }
    }

    @Override
    public void close() {
        currentReader.close();
        journal.closeReader(this);
    }
}
