package org.logstash.cluster.storage.buffer;

import com.google.common.base.Preconditions;
import org.logstash.cluster.utils.memory.DirectMemoryAllocator;
import org.logstash.cluster.utils.memory.HeapMemory;
import org.logstash.cluster.utils.memory.Memory;

/**
 * Direct {@link java.nio.ByteBuffer} based buffer.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBuffer extends ByteBufferBuffer {

    protected DirectBuffer(DirectBytes bytes, int offset, int initialCapacity, int maxCapacity) {
        super(bytes, offset, initialCapacity, maxCapacity, null);
    }

    /**
     * Allocates a direct buffer with an initial capacity of {@code 4096} and a maximum capacity of {@link Long#MAX_VALUE}.
     * <p>
     * When the buffer is constructed, {@link DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will be initialized with a capacity of {@code 4096}
     * and have a maximum capacity of {@link Long#MAX_VALUE}. The buffer's {@code capacity} will dynamically expand as
     * bytes are written to the buffer. The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
     * @return The direct buffer.
     * @see DirectBuffer#allocate(int)
     * @see DirectBuffer#allocate(int, int)
     */
    public static DirectBuffer allocate() {
        return allocate(DEFAULT_INITIAL_CAPACITY, HeapMemory.MAX_SIZE);
    }

    /**
     * Allocates a new direct buffer.
     * <p>
     * When the buffer is constructed, {@link DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code initialCapacity}
     * and will be doubled up to {@code maxCapacity} as bytes are written to the buffer. The underlying {@link UnsafeDirectBytes}
     * will be initialized to the next power of {@code 2}.
     * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
     * @param maxCapacity The maximum capacity of the buffer.
     * @return The direct buffer.
     * @throws IllegalArgumentException If {@code capacity} or {@code maxCapacity} is greater than the maximum
     * allowed count for a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
     * @see DirectBuffer#allocate()
     * @see DirectBuffer#allocate(int)
     */
    public static DirectBuffer allocate(int initialCapacity, int maxCapacity) {
        Preconditions.checkArgument(initialCapacity <= maxCapacity, "initial capacity cannot be greater than maximum capacity");
        return new DirectBuffer(DirectBytes.allocate((int) Math.min(Memory.Util.toPow2(initialCapacity), HeapMemory.MAX_SIZE)), 0, initialCapacity, maxCapacity);
    }

    /**
     * Allocates a direct buffer with the given initial capacity.
     * <p>
     * When the buffer is constructed, {@link DirectMemoryAllocator} will be used to allocate
     * {@code capacity} bytes of off-heap memory. The resulting buffer will have an initial capacity of {@code capacity}.
     * The underlying {@link UnsafeDirectBytes} will be initialized to the next power of {@code 2}.
     * @param initialCapacity The initial capacity of the buffer to allocate (in bytes).
     * @return The direct buffer.
     * @throws IllegalArgumentException If {@code capacity} is greater than the maximum allowed count for
     * a {@link java.nio.ByteBuffer} - {@code Integer.MAX_VALUE - 5}
     * @see DirectBuffer#allocate()
     * @see DirectBuffer#allocate(int, int)
     */
    public static DirectBuffer allocate(int initialCapacity) {
        return allocate(initialCapacity, HeapMemory.MAX_SIZE);
    }

    @Override
    public DirectBuffer duplicate() {
        return new DirectBuffer((DirectBytes) bytes, offset(), capacity(), maxCapacity());
    }
}
