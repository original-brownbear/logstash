package org.logstash.cluster.storage.journal;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.logstash.cluster.serializer.Serializer;

/**
 * Log segment.
 */
public class JournalSegment<E> implements AutoCloseable {
    protected final JournalSegmentFile file;
    protected final JournalSegmentDescriptor descriptor;
    protected final Serializer serializer;
    private final JournalSegmentWriter<E> writer;
    private boolean open = true;

    public JournalSegment(JournalSegmentFile file, JournalSegmentDescriptor descriptor, Serializer serializer) {
        this.file = file;
        this.descriptor = descriptor;
        this.serializer = serializer;
        this.writer = new JournalSegmentWriter<>(descriptor, serializer);
    }

    /**
     * Returns the last index in the segment.
     * @return The last index in the segment.
     */
    public long lastIndex() {
        return writer.getLastIndex();
    }

    /**
     * Returns the segment file.
     * @return The segment file.
     */
    public JournalSegmentFile file() {
        return file;
    }

    /**
     * Returns the segment descriptor.
     * @return The segment descriptor.
     */
    public JournalSegmentDescriptor descriptor() {
        return descriptor;
    }

    /**
     * Returns the segment size.
     * @return The segment size.
     */
    public long size() {
        return writer.size();
    }

    /**
     * Returns a boolean value indicating whether the segment is empty.
     * @return Indicates whether the segment is empty.
     */
    public boolean isEmpty() {
        return length() == 0;
    }

    /**
     * Returns the segment length.
     * @return The segment length.
     */
    public long length() {
        return writer.getNextIndex() - index();
    }

    /**
     * Returns the segment's starting index.
     * @return The segment's starting index.
     */
    public long index() {
        return descriptor.index();
    }

    /**
     * Returns a boolean indicating whether the segment is full.
     * @return Indicates whether the segment is full.
     */
    public boolean isFull() {
        return writer.isFull();
    }

    /**
     * Returns the segment writer.
     * @return The segment writer.
     */
    public JournalSegmentWriter<E> writer() {
        checkOpen();
        return writer;
    }

    /**
     * Checks whether the segment is open.
     */
    private void checkOpen() {
        Preconditions.checkState(open, "Segment not open");
    }

    /**
     * Creates a new segment reader.
     * @return A new segment reader.
     */
    JournalSegmentReader<E> createReader() {
        checkOpen();
        return new JournalSegmentReader<>(descriptor, serializer);
    }

    /**
     * Returns a boolean indicating whether the segment is open.
     * @return indicates whether the segment is open
     */
    public boolean isOpen() {
        return open;
    }

    /**
     * Closes the segment.
     */
    @Override
    public void close() {
        writer.close();
        descriptor.close();
        open = false;
    }

    /**
     * Deletes the segment.
     */
    public void delete() {
        writer.delete();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id())
            .add("version", version())
            .toString();
    }

    /**
     * Returns the segment ID.
     * @return The segment ID.
     */
    public long id() {
        return descriptor.id();
    }

    /**
     * Returns the segment version.
     * @return The segment version.
     */
    public long version() {
        return descriptor.version();
    }
}
