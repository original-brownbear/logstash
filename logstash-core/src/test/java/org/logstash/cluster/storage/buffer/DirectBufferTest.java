package org.logstash.cluster.storage.buffer;

import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Direct buffer test.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBufferTest extends BufferTest {

    @Override
    protected Buffer createBuffer(int capacity) {
        return DirectBuffer.allocate(capacity);
    }

    @Override
    protected Buffer createBuffer(int capacity, int maxCapacity) {
        return DirectBuffer.allocate(capacity, maxCapacity);
    }

    @Test
    public void testByteBufferToDirectBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(10);
        byteBuffer.flip();

        HeapBuffer directBuffer = HeapBuffer.allocate(8);
        directBuffer.write(byteBuffer.array());
        directBuffer.flip();
        assertEquals(directBuffer.readLong(), byteBuffer.getLong());

        byteBuffer.rewind();
        UnsafeHeapBuffer heapBuffer = UnsafeHeapBuffer.wrap(byteBuffer.array());
        assertEquals(heapBuffer.readLong(), byteBuffer.getLong());
    }

    @Test
    public void testUnsafeDirectToDirectBuffer() {
        UnsafeDirectBuffer unsafeDirectBuffer = UnsafeDirectBuffer.allocate(8);
        unsafeDirectBuffer.writeLong(10);
        unsafeDirectBuffer.flip();

        byte[] bytes = new byte[8];
        unsafeDirectBuffer.read(bytes);
        unsafeDirectBuffer.rewind();

        DirectBuffer directBuffer = DirectBuffer.allocate(8);
        directBuffer.write(bytes);
        directBuffer.flip();
        assertEquals(unsafeDirectBuffer.readLong(), directBuffer.readLong());
    }

    @Test
    public void testDirectToUnsafeDirectBuffer() {
        DirectBuffer directBuffer = DirectBuffer.allocate(8);
        directBuffer.writeLong(10);
        directBuffer.flip();

        byte[] bytes = new byte[8];
        directBuffer.read(bytes);
        directBuffer.rewind();

        UnsafeDirectBuffer unsafeDirectBuffer = UnsafeDirectBuffer.allocate(8);
        unsafeDirectBuffer.write(bytes);
        unsafeDirectBuffer.flip();

        assertEquals(unsafeDirectBuffer.readLong(), directBuffer.readLong());
    }

}
