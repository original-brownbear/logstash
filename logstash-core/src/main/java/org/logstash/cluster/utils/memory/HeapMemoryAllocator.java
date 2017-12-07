package org.logstash.cluster.utils.memory;

/**
 * Java heap memory allocator.
 */
public class HeapMemoryAllocator implements MemoryAllocator<HeapMemory> {

    @Override
    public HeapMemory allocate(int size) {
        if (size > Integer.MAX_VALUE)
            throw new IllegalArgumentException("size cannot be greater than " + Integer.MAX_VALUE);
        return new HeapMemory(new byte[size], this);
    }

    @Override
    public HeapMemory reallocate(HeapMemory memory, int size) {
        HeapMemory copy = allocate(size);
        NativeMemory.UNSAFE.copyMemory(memory.array(), HeapMemory.ARRAY_BASE_OFFSET, copy.array(), HeapMemory.ARRAY_BASE_OFFSET, Math.min(size, memory.size()));
        memory.free();
        return copy;
    }

}
