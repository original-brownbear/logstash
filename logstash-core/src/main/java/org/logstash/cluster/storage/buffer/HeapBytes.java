package org.logstash.cluster.storage.buffer;

import java.nio.ByteBuffer;
import org.logstash.cluster.utils.memory.HeapMemory;

/**
 * {@link ByteBuffer} based heap bytes.
 */
public class HeapBytes extends ByteBufferBytes {
    public static final byte[] EMPTY = new byte[0];

    protected HeapBytes(ByteBuffer buffer) {
        super(buffer);
    }

    /**
     * Allocates a new heap byte array.
     * @param size The count of the buffer to allocate (in bytes).
     * @return The heap buffer.
     * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
     * an array on the Java heap - {@code Integer.MAX_VALUE - 5}
     */
    public static HeapBytes allocate(int size) {
        if (size > HeapMemory.MAX_SIZE)
            throw new IllegalArgumentException("size cannot for HeapBytes cannot be greater than " + HeapMemory.MAX_SIZE);
        return new HeapBytes(ByteBuffer.allocate(size));
    }

    /**
     * Wraps the given bytes in a {@link UnsafeHeapBytes} object.
     * <p>
     * The returned {@link Bytes} object will be backed by a {@link HeapMemory} instance that
     * wraps the given byte array. The {@link Bytes#size()} will be equivalent to the provided
     * by array {@code length}.
     * @param bytes The bytes to wrap.
     */
    public static HeapBytes wrap(byte[] bytes) {
        return new HeapBytes(ByteBuffer.wrap(bytes));
    }

    @Override
    protected ByteBuffer newByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public boolean hasArray() {
        return true;
    }
}
