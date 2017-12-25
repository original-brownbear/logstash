package org.logstash.cluster.storage.buffer;

/**
 * Direct buffer test.
 */
public class UnsafeDirectBufferTest extends BufferTest {

    @Override
    protected Buffer createBuffer(int capacity) {
        return UnsafeDirectBuffer.allocate(capacity);
    }

    @Override
    protected Buffer createBuffer(int capacity, int maxCapacity) {
        return UnsafeDirectBuffer.allocate(capacity, maxCapacity);
    }

}
