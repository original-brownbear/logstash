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
 * File buffer test.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileBufferTest extends BufferTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected Buffer createBuffer(int capacity) {
        try {
            return FileBuffer.allocate(temporaryFolder.newFile(), capacity);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    protected Buffer createBuffer(int capacity, int maxCapacity) {
        try {
            return FileBuffer.allocate(temporaryFolder.newFile(), capacity, maxCapacity);
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Test
    public void testFileToHeapBuffer() throws IOException {
        File file = temporaryFolder.newFile();
        try (FileBuffer buffer = FileBuffer.allocate(file, 16)) {
            buffer.writeLong(10).writeLong(11).flip();
            byte[] bytes = new byte[16];
            buffer.read(bytes).rewind();
            HeapBuffer heapBuffer = HeapBuffer.wrap(bytes);
            assertEquals(buffer.readLong(), heapBuffer.readLong());
            assertEquals(buffer.readLong(), heapBuffer.readLong());
        }
    }

    /**
     * Rests reopening a file that has been closed.
     */
    @Test
    public void testPersist() throws IOException {
        File file = temporaryFolder.newFile();
        try (FileBuffer buffer = FileBuffer.allocate(file, 16)) {
            buffer.writeLong(10).writeLong(11).flip();
            assertEquals(buffer.readLong(), 10);
            assertEquals(buffer.readLong(), 11);
        }
        try (FileBuffer buffer = FileBuffer.allocate(file, 16)) {
            assertEquals(buffer.readLong(), 10);
            assertEquals(buffer.readLong(), 11);
        }
    }

    /**
     * Tests deleting a file.
     */
    @Test
    public void testDelete() throws IOException {
        File file = temporaryFolder.newFile();
        FileBuffer buffer = FileBuffer.allocate(file, 16);
        buffer.writeLong(10).writeLong(11).flip();
        assertEquals(buffer.readLong(), 10);
        assertEquals(buffer.readLong(), 11);
        assertTrue(Files.exists(file.toPath()));
        buffer.delete();
        assertFalse(Files.exists(file.toPath()));
    }

}
