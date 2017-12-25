package org.logstash.cluster.storage.buffer;

import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Heap buffer test.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapBufferTest extends BufferTest {

    @Override
    protected Buffer createBuffer(int capacity) {
        return HeapBuffer.allocate(capacity);
    }

    @Override
    protected Buffer createBuffer(int capacity, int maxCapacity) {
        return HeapBuffer.allocate(capacity, maxCapacity);
    }

    @Test
    public void testByteBufferToHeapBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(10);
        byteBuffer.rewind();

        HeapBuffer directBuffer = HeapBuffer.wrap(byteBuffer.array());
        assertEquals(directBuffer.readLong(), byteBuffer.getLong());

        byteBuffer.rewind();
        UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.wrap(byteBuffer.array());
        assertEquals(heapBuffer.readLong(), byteBuffer.getLong());
    }

    @Test
    public void testDirectToHeapBuffer() {
        DirectBuffer directBuffer = DirectBuffer.allocate(8);
        directBuffer.writeLong(10);
        directBuffer.flip();

        byte[] bytes = new byte[8];
        directBuffer.read(bytes);
        directBuffer.rewind();

        HeapBuffer heapBuffer = HeapBuffer.wrap(bytes);
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
