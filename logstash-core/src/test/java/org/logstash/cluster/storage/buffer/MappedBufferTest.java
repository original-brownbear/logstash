package org.logstash.cluster.storage.buffer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Mapped buffer test.
 */
public class MappedBufferTest extends BufferTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected Buffer createBuffer(final int capacity) {
        try {
            return MappedBuffer.allocate(temporaryFolder.newFile(), capacity);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    protected Buffer createBuffer(final int capacity, final int maxCapacity) {
        try {
            return MappedBuffer.allocate(temporaryFolder.newFile(), capacity, maxCapacity);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Rests reopening a file that has been closed.
     */
    @Test
    public void testPersist() throws IOException {
        final File file = temporaryFolder.newFile();
        try (MappedBuffer buffer = MappedBuffer.allocate(file, 16)) {
            buffer.writeLong(10).writeLong(11).flip();
            assertEquals(buffer.readLong(), 10);
            assertEquals(buffer.readLong(), 11);
        }
        try (MappedBuffer buffer = MappedBuffer.allocate(file, 16)) {
            assertEquals(buffer.readLong(), 10);
            assertEquals(buffer.readLong(), 11);
        }
    }

    /**
     * Tests deleting a file.
     */
    @Test
    public void testDelete() throws IOException {
        final File file = temporaryFolder.newFile();
        final MappedBuffer buffer = MappedBuffer.allocate(file, 16);
        buffer.writeLong(10).writeLong(11).flip();
        assertEquals(buffer.readLong(), 10);
        assertEquals(buffer.readLong(), 11);
        assertTrue(Files.exists(file.toPath()));
        buffer.delete();
        assertFalse(Files.exists(file.toPath()));
    }

}
