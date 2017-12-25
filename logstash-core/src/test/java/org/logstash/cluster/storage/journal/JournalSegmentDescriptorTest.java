package org.logstash.cluster.storage.journal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Test;
import org.logstash.cluster.storage.buffer.Buffer;
import org.logstash.cluster.storage.buffer.FileBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Segment descriptor test.
 */
public class JournalSegmentDescriptorTest {
    private static final File file = new File("descriptor.log");

    /**
     * Tests the segment descriptor builder.
     */
    @Test
    public void testDescriptorBuilder() {
        JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES))
            .withId(2)
            .withIndex(1025)
            .withMaxSegmentSize(1024 * 1024)
            .withMaxEntries(2048)
            .build();

        assertEquals(descriptor.id(), 2);
        assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
        assertEquals(descriptor.index(), 1025);
        assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
        assertEquals(descriptor.maxEntries(), 2048);

        assertEquals(descriptor.updated(), 0);
        long time = System.currentTimeMillis();
        descriptor.update(time);
        assertEquals(descriptor.updated(), time);
    }

    /**
     * Tests persisting the segment descriptor.
     */
    @Test
    public void testDescriptorPersist() {
        Buffer buffer = FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES);
        JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder(buffer)
            .withId(2)
            .withIndex(1025)
            .withMaxSegmentSize(1024 * 1024)
            .withMaxEntries(2048)
            .build();

        assertEquals(descriptor.id(), 2);
        assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
        assertEquals(descriptor.index(), 1025);
        assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
        assertEquals(descriptor.maxEntries(), 2048);

        buffer.close();

        descriptor = new JournalSegmentDescriptor(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

        assertEquals(descriptor.id(), 2);
        assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
        assertEquals(descriptor.index(), 1025);
        assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);

        descriptor.close();

        descriptor = new JournalSegmentDescriptor(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

        assertEquals(descriptor.id(), 2);
        assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
        assertEquals(descriptor.index(), 1025);
        assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
    }

    /**
     * Tests copying the segment descriptor.
     */
    @Test
    public void testDescriptorCopy() {
        JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
            .withId(2)
            .withIndex(1025)
            .withMaxSegmentSize(1024 * 1024)
            .withMaxEntries(2048)
            .build();

        long time = System.currentTimeMillis();
        descriptor.update(time);

        descriptor = descriptor.copyTo(FileBuffer.allocate(file, JournalSegmentDescriptor.BYTES));

        assertEquals(descriptor.id(), 2);
        assertEquals(descriptor.version(), JournalSegmentDescriptor.VERSION);
        assertEquals(descriptor.index(), 1025);
        assertEquals(descriptor.maxSegmentSize(), 1024 * 1024);
        assertEquals(descriptor.maxEntries(), 2048);
        assertEquals(descriptor.updated(), time);
    }

    /**
     * Deletes the descriptor file.
     */
    @After
    public void deleteDescriptor() throws IOException {
        if (Files.exists(file.toPath())) {
            Files.delete(file.toPath());
        }
    }
}
