package org.logstash.cluster.utils.memory;

/**
 * Memory allocator.
 * <p>
 * Memory allocators handle allocation of memory for {@link org.logstash.cluster.storage.buffer.Bytes} objects, providing descriptors
 * that point to memory addresses.
 */
public interface MemoryAllocator<T extends Memory> {

    /**
     * Allocates memory.
     * @param size The count of the memory to allocate.
     * @return The allocated memory.
     */
    T allocate(int size);

    /**
     * Reallocates the given memory.
     * <p>
     * When the memory is reallocated, the memory address for the given {@link Memory} instance may change. The returned
     * {@link Memory} instance will contain the updated address and count.
     * @param memory The memory to reallocate.
     * @param size The count to which to reallocate the given memory.
     * @return The reallocated memory.
     */
    T reallocate(T memory, int size);
}