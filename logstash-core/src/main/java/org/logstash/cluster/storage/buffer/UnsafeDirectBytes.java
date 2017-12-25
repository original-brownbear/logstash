package org.logstash.cluster.storage.buffer;

import org.logstash.cluster.utils.memory.DirectMemory;

/**
 * Direct byte buffer bytes.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnsafeDirectBytes extends NativeBytes {

    protected UnsafeDirectBytes(DirectMemory memory) {
        super(memory);
    }

    /**
     * Allocates a direct {@link java.nio.ByteBuffer} based byte array.
     * <p>
     * When the array is constructed, {@link org.logstash.cluster.utils.memory.DirectMemoryAllocator} will be used to allocate
     * {@code count} bytes of off-heap memory. Memory is accessed by the buffer directly via {@link sun.misc.Unsafe}.
     * @param size The count of the buffer to allocate (in bytes).
     * @return The native buffer.
     * @throws IllegalArgumentException If {@code count} is greater than the maximum allowed count for
     * a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
     */
    public static UnsafeDirectBytes allocate(int size) {
        return new UnsafeDirectBytes(DirectMemory.allocate(size));
    }

    /**
     * Copies the bytes to a new byte array.
     * @return A new {@link UnsafeHeapBytes} instance backed by a copy of this instance's array.
     */
    public UnsafeDirectBytes copy() {
        return new UnsafeDirectBytes((DirectMemory) memory.copy());
    }

}
