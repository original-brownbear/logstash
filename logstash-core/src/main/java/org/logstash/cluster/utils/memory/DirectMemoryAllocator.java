package org.logstash.cluster.utils.memory;

/**
 * Direct memory allocator.
 */
public class DirectMemoryAllocator implements MemoryAllocator<NativeMemory> {

    @Override
    public DirectMemory allocate(int size) {
        DirectMemory memory = new DirectMemory(DirectMemory.UNSAFE.allocateMemory(size), size, this);
        DirectMemory.UNSAFE.setMemory(memory.address(), size, (byte) 0);
        return memory;
    }

    @Override
    public DirectMemory reallocate(NativeMemory memory, int size) {
        DirectMemory newMemory = new DirectMemory(DirectMemory.UNSAFE.reallocateMemory(memory.address(), size), size, this);
        if (newMemory.size() > memory.size()) {
            DirectMemory.UNSAFE.setMemory(newMemory.address(), newMemory.size() - memory.size(), (byte) 0);
        }
        return newMemory;
    }

}
