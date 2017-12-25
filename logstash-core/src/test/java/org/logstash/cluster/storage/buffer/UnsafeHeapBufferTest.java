package org.logstash.cluster.storage.buffer;

import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Heap buffer test.
 */
public class UnsafeHeapBufferTest extends BufferTest {

    @Override
    protected Buffer createBuffer(int capacity) {
        return UnsafeHeapBuffer.allocate(capacity);
    }

    @Override
    protected Buffer createBuffer(int capacity, int maxCapacity) {
        return UnsafeHeapBuffer.allocate(capacity, maxCapacity);
    }

    @Test
    public void testByteBufferToHeapBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(10);
        byteBuffer.flip();

        UnsafeDirectBuffer directBuffer = UnsafeDirectBuffer.allocate(8);
        directBuffer.write(byteBuffer.array());
        directBuffer.flip();
        assertEquals(directBuffer.readLong(), byteBuffer.getLong());

        byteBuffer.rewind();
        UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.wrap(byteBuffer.array());
        assertEquals(heapBuffer.readLong(), byteBuffer.getLong());
    }

    @Test
    public void testDirectToHeapBuffer() {
        UnsafeDirectBuffer directBuffer = UnsafeDirectBuffer.allocate(8);
        directBuffer.writeLong(10);
        directBuffer.flip();

        byte[] bytes = new byte[8];
        directBuffer.read(bytes);
        directBuffer.rewind();

        UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.wrap(bytes);
        assertEquals(directBuffer.readLong(), heapBuffer.readLong());
    }

    @Test
    public void testHeapToDirectBuffer() {
        UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.allocate(8);
        heapBuffer.writeLong(10);
        heapBuffer.flip();

        UnsafeDirectBuffer directBuffer = UnsafeDirectBuffer.allocate(8);
        directBuffer.write(heapBuffer.array());
        directBuffer.flip();

        assertEquals(directBuffer.readLong(), heapBuffer.readLong());
    }

}
