package org.logstash.cluster.storage.buffer;

import java.nio.ByteBuffer;

/**
 * {@link ByteBuffer} based direct bytes.
 */
public class DirectBytes extends ByteBufferBytes {

    protected DirectBytes(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Allocates a new direct byte array.
     * @param size The count of the buffer to allocate (in bytes).
     * @return The direct buffer.
     * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
     * an array on the Java heap - {@code Integer.MAX_VALUE - 5}
     */
    public static DirectBytes allocate(int size) {
        return new DirectBytes(ByteBuffer.allocate(size));
    }

    @Override
    protected ByteBuffer newByteBuffer(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    @Override
    public boolean isDirect() {
        return true;
    }

}
