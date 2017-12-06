package org.logstash.cluster.utils.memory;

/**
 * Direct memory.
 */
public class DirectMemory extends NativeMemory {

    public DirectMemory(long address, int size, DirectMemoryAllocator allocator) {
        super(address, size, allocator);
    }

    /**
     * Allocates direct memory via {@link DirectMemoryAllocator}.
     * @param size The count of the memory to allocate.
     * @return The allocated memory.
     */
    public static DirectMemory allocate(int size) {
        return new DirectMemoryAllocator().allocate(size);
    }

}
