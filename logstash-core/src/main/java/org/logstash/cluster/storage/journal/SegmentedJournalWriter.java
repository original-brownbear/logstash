package org.logstash.cluster.storage.journal;

/**
 * Log writer.
 */
public class SegmentedJournalWriter<E> implements JournalWriter<E> {
    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private JournalSegmentWriter<E> currentWriter;

    public SegmentedJournalWriter(SegmentedJournal<E> journal) {
        this.journal = journal;
        this.currentSegment = journal.getLastSegment();
        this.currentWriter = currentSegment.writer();
    }

    @Override
    public long getLastIndex() {
        return currentWriter.getLastIndex();
    }

    @Override
    public Indexed<E> getLastEntry() {
        return currentWriter.getLastEntry();
    }

    @Override
    public long getNextIndex() {
        return currentWriter.getNextIndex();
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        if (currentWriter.isFull()) {
            currentSegment = journal.getNextSegment();
            currentWriter = currentSegment.writer();
        }
        return currentWriter.append(entry);
    }

    @Override
    public void append(Indexed<E> entry) {
        if (currentWriter.isFull()) {
            currentSegment = journal.getNextSegment();
            currentWriter = currentSegment.writer();
        }
        currentWriter.append(entry);
    }

    @Override
    public void truncate(long index) {
        // Delete all segments with first indexes greater than the given index.
        while (index < currentWriter.firstIndex() - 1) {
            currentWriter.close();
            journal.removeSegment(currentSegment);
            currentSegment = journal.getLastSegment();
            currentWriter = currentSegment.writer();
        }

        // Truncate the current index.
        currentWriter.truncate(index);

        // Reset segment readers.
        journal.resetTail(index + 1);
    }

    @Override
    public void flush() {
        currentWriter.flush();
    }

    @Override
    public void close() {
        currentWriter.close();
    }

    /**
     * Resets the head of the journal to the given index.
     * @param index the index to which to reset the head of the journal
     */
    public void reset(long index) {
        currentWriter.close();
        currentSegment = journal.resetSegments(index);
        currentWriter = currentSegment.writer();
        journal.resetHead(index);
    }
}
